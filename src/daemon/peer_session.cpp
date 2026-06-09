#include "peer_session.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "common/net_util.hpp"
#include "common/file_util.hpp"
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <cmath>

namespace thinbt {

PeerSession::PeerSession(asio::io_context& io, const Sha1Digest& info_hash, uint32_t local_speed_mbps)
    : socket_(std::make_shared<asio::ip::tcp::socket>(io))
    , local_speed_mbps_(local_speed_mbps)
    , eval_timer_(std::make_unique<asio::steady_timer>(io))
    , last_eval_time_(std::chrono::steady_clock::now())
    , last_choke_eval_time_(std::chrono::steady_clock::now())
{
    memcpy(info_hash_.data(), info_hash.data(), 20);
}

PeerSession::~PeerSession() = default;

std::string PeerSession::remote_ip() const {
    asio::error_code ec;
    auto ep = socket_->remote_endpoint(ec);
    return ec ? "unknown" : ep.address().to_string();
}

uint16_t PeerSession::remote_port() const {
    asio::error_code ec;
    auto ep = socket_->remote_endpoint(ec);
    return ec ? 0 : ep.port();
}

// ── Inbound ──
void PeerSession::start_inbound(asio::ip::tcp::socket sock, OnDisconnect on_disc) {
    *socket_ = std::move(sock);
    on_disconnect_ = std::move(on_disc);
    start_read_handshake();
}

// ── Outbound ──
void PeerSession::start_outbound(const std::string& host, uint16_t port, OnDisconnect on_disc) {
    on_disconnect_ = std::move(on_disc);
    auto self = shared_from_this();
    asio::ip::tcp::resolver resolver(socket_->get_executor());
    auto endpoints = resolver.resolve(host, std::to_string(port));
    asio::async_connect(*socket_, endpoints,
        [self](asio::error_code ec, auto) {
            if (ec) { self->disconnect(); return; }
            self->send_handshake();
            self->start_read_handshake();
        });
}

// ── Handshake ──
void PeerSession::send_handshake() {
    Handshake h;
    h.build(info_hash_, local_speed_mbps_);
    for (int i = 0; i < 20; i++) h.peer_id[i] = static_cast<uint8_t>(rand() % 256);
    send_message(serialize_handshake(h));
    handshake_sent_ = true;
}

void PeerSession::start_read_handshake() {
    auto self = shared_from_this();
    asio::async_read(*socket_, asio::buffer(handshake_buf_, 67),
        [self](asio::error_code ec, size_t) { self->handle_handshake(ec, 0); });
}

void PeerSession::handle_handshake(const asio::error_code& ec, size_t) {
    if (ec) { disconnect(); return; }

    Handshake h;
    if (!parse_handshake(handshake_buf_.data(), 67, h)) { disconnect(); return; }
    if (memcmp(h.info_hash, info_hash_.data(), 20) != 0) { disconnect(); return; }

    remote_speed_mbps_ = h.speed_mbps;

    if (!handshake_sent_) send_handshake();

    state_ = State::CONNECTED;
    start_eval_timer();
    start_read_header();
    if (on_handshake_done_) on_handshake_done_(shared_from_this());
}

// ── Message read chain ──
void PeerSession::start_read_header() {
    auto self = shared_from_this();
    asio::async_read(*socket_, asio::buffer(header_buf_, 5),
        [self](asio::error_code ec, size_t) {
            if (ec) { self->disconnect(); return; }
            uint32_t msg_len; P2PMsgId id;
            if (!parse_message_header(self->header_buf_.data(), 5, msg_len, id)) {
                self->disconnect(); return;
            }
            uint32_t body_len = msg_len - 1;
            if (body_len > PeerSession::MAX_MSG_SIZE) {
                self->disconnect(); return;
            }
            self->start_read_body(body_len, id);
        });
}

void PeerSession::start_read_body(uint32_t body_len, P2PMsgId msg_id) {
    auto self = shared_from_this();
    if (body_len == 0) {
        self->dispatch_message(msg_id, nullptr, 0);
        self->start_read_header();
        return;
    }
    auto body = std::make_shared<std::vector<uint8_t>>(body_len);
    asio::async_read(*socket_, asio::buffer(*body),
        [self, body, msg_id](asio::error_code ec, size_t) {
            if (ec) { self->disconnect(); return; }
            self->current_body_ = body;
            self->dispatch_message(msg_id, body->data(), static_cast<uint32_t>(body->size()));
            self->current_body_.reset();
            self->start_read_header();
        });
}

void PeerSession::dispatch_message(P2PMsgId id, const uint8_t* data, uint32_t len) {
    switch (id) {
    case P2PMsgId::CHOKE:          am_choked_.store(true, std::memory_order_release);  break;
    case P2PMsgId::UNCHOKE:
        am_choked_.store(false, std::memory_order_release);
        if (scheduler_ && scheduler_->missing_count() > 0)
            send_interested();
        break;
    case P2PMsgId::INTERESTED:     peer_interested_.store(true, std::memory_order_release);  break;
    case P2PMsgId::NOT_INTERESTED: peer_interested_.store(false, std::memory_order_release); break;
    case P2PMsgId::HAVE:           handle_have_msg(data);   break;
    case P2PMsgId::BITFIELD:       handle_bitfield_msg(data, len); break;
    case P2PMsgId::REQUEST:        handle_request_msg(data); break;
    case P2PMsgId::PIECE:          handle_piece_msg(data, len); break;
    case P2PMsgId::CANCEL:         handle_cancel_msg(data);  break;
    case P2PMsgId::PEX:            handle_pex_msg(data, len); break;
    default: break;
    }
}

// ── Message handlers ──
void PeerSession::handle_have_msg(const uint8_t* data) {
    uint32_t ci; memcpy(&ci, data, 4); ci = ntoh32(ci);
    record_have(ci);
    if (scheduler_) scheduler_->on_have(slot_id_, ci);
}

void PeerSession::handle_bitfield_msg(const uint8_t* data, uint32_t len) {
    record_bitfield(data, len);
    if (scheduler_) scheduler_->on_bitfield(slot_id_, remote_bitfield_);
}

void PeerSession::handle_request_msg(const uint8_t* data) {
    if (am_choked_.load(std::memory_order_acquire)) return;

    uint32_t index, begin, length;
    memcpy(&index, data, 4);     index  = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin  = ntoh32(begin);
    memcpy(&length, data + 8, 4); length = ntoh32(length);

    // CANCEL check — skip if this sub-block was cancelled (Endgame cleanup)
    if (cancelled_set_.count({index, begin})) return;

    if (file_fd_ >= 0) {
        uint64_t base_off = (chunk_offsets_ && index < chunk_offsets_->size())
                            ? (*chunk_offsets_)[index] : index * 131072ULL;
        uint64_t file_off = base_off + begin;

        // 使用 pread() 而非 sendfile(2):
        // sendfile(2) 是阻塞系统调用，LAN 场景下 16KB 子块从 page cache
        // 读取耗时 < 1μs，额外内存拷贝开销可忽略。在 asio 事件循环中直接
        // 调用 sendfile(2) 会阻塞所有 Peer 连接，收益不抵风险。
        // 真零拷贝需 Linux splice(2)+SPLICE_F_NONBLOCK+pipe 组合，复杂度高。
        std::vector<uint8_t> buf(length);
        ssize_t n = thinbt_pread(file_fd_, buf.data(), length, static_cast<off_t>(file_off));
        if (n > 0) {
            auto msg = build_piece(index, begin, buf.data(), static_cast<uint32_t>(n));
            send_message(std::move(msg));
        }
    }
}

void PeerSession::handle_cancel_msg(const uint8_t* data) {
    uint32_t index, begin;
    memcpy(&index, data, 4);     index = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin = ntoh32(begin);
    cancelled_set_.insert({index, begin});
}

void PeerSession::handle_pex_msg(const uint8_t* data, uint32_t len) {
    // PEX payload: [op:1][count:2][PexPeer × count]
    // op=0x00: 全量交换，对非 left Peer 尝试连接
    // op=0x01: 增量交换，left Peer 通知移除，其他 Peer 选择性连接
    if (len < 3) return;
    uint8_t  op    = data[0];
    uint16_t count; memcpy(&count, data + 1, 2); count = ntoh16(count);
    if (len < 3u + count * 8) return;

    for (uint16_t i = 0; i < count; i++) {
        PexPeer p;
        memcpy(&p, data + 3 + i * 8, 8);
        p.ip   = ntoh32(p.ip);
        p.port = ntoh16(p.port);

        bool is_left = (p.flags & 0x80) != 0;

        if (op == 0x01 && is_left) {
            // Delta: 对端离开，通知 PeerManager 从本地池移除
            if (on_pex_remove_) {
                struct in_addr ia; ia.s_addr = p.ip;
                on_pex_remove_(inet_ntoa(ia), p.port);
            }
        } else if (!is_left && on_pex_peer_) {
            // 全量: 忽略 left Peer；增量: 新加入的 Peer 尝试连接
            struct in_addr ia; ia.s_addr = p.ip;
            on_pex_peer_(inet_ntoa(ia), p.port, p.flags);
        }
    }
}

void PeerSession::handle_piece_msg(const uint8_t* data, uint32_t len) {
    if (len < 8 || !io_pool_) return;
    uint32_t index, begin;
    memcpy(&index, data, 4); index = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin = ntoh32(begin);

    // Fast Fail: 收到数据 → 清除超时计数
    consecutive_timeouts_ = 0;

    // 动态 Pipeline: 记录响应延迟
    auto it = pending_times_.find({index, begin});
    if (it != pending_times_.end()) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - it->second).count();
        // EMA: alpha=0.3 平滑延迟估计
        avg_latency_us_ = LATENCY_EMA_ALPHA * elapsed_us + (1.0 - LATENCY_EMA_ALPHA) * avg_latency_us_;
        pending_times_.erase(it);
    }

    // Choke Tit-for-tat: 累计接收字节
    recv_bytes_since_last_choke_ += (len - 8);

    PieceTask task{index, begin, len - 8, slot_id_, data + 8, current_body_};
    io_pool_->dispatch(task);
    dec_pending();
    if (scheduler_) scheduler_->dec_peer_pending(slot_id_, index, begin);
}

void PeerSession::record_have(uint32_t chunk_idx) {
    if (chunk_idx < remote_bitfield_.size())
        remote_bitfield_[chunk_idx] = true;
}

void PeerSession::record_bitfield(const uint8_t* data, uint32_t len) {
    remote_bitfield_.resize(len * 8);
    for (uint32_t i = 0; i < len * 8; i++)
        remote_bitfield_[i] = (data[i / 8] >> (7 - (i % 8))) & 1;
}

// ── Write serialization ──
void PeerSession::send_message(std::vector<uint8_t> buf) {
    bool trigger = false;
    {
        std::lock_guard<std::mutex> lock(write_mtx_);
        write_queue_.push_back(std::move(buf));
        if (!is_writing_) {
            is_writing_ = true;
            trigger = true;
        }
    }
    if (trigger) do_write();
}

void PeerSession::do_write() {
    auto self = shared_from_this();
    std::vector<uint8_t> front;
    {
        std::lock_guard<std::mutex> lock(write_mtx_);
        if (write_queue_.empty()) { is_writing_ = false; return; }
        front.swap(write_queue_.front()); // swap to avoid holding lock during async
    }
    auto buf = asio::buffer(front);
    asio::async_write(*socket_, buf,
        [self, front = std::move(front)](asio::error_code ec, size_t) mutable {
            if (ec) { self->disconnect(); return; }
            bool has_more = false;
            {
                std::lock_guard<std::mutex> lk(self->write_mtx_);
                self->write_queue_.pop_front();
                has_more = !self->write_queue_.empty();
                if (!has_more) self->is_writing_ = false;
            }
            if (has_more) self->do_write();
        });
}

void PeerSession::disconnect() {
    if (state_ == State::DISCONNECTED) return;
    state_ = State::DISCONNECTED;
    asio::error_code ec;
    if (eval_timer_) eval_timer_->cancel(ec);
    socket_->close(ec);
    if (on_disconnect_) on_disconnect_(shared_from_this());
}

void PeerSession::send_interested() {
    if (am_interested_.load(std::memory_order_acquire)) return;
    am_interested_.store(true, std::memory_order_release);
    send_message(build_interested());
}

void PeerSession::send_not_interested() {
    if (!am_interested_.load(std::memory_order_acquire)) return;
    am_interested_.store(false, std::memory_order_release);
    send_message(build_not_interested());
}

// ── Fast Fail + 动态 Pipeline ──
void PeerSession::record_request_sent(uint32_t chunk_idx, uint32_t begin) {
    pending_times_[{chunk_idx, begin}] = std::chrono::steady_clock::now();
}

void PeerSession::start_eval_timer() {
    last_eval_time_ = std::chrono::steady_clock::now();
    auto self = shared_from_this();
    eval_timer_->expires_after(std::chrono::milliseconds(PIPELINE_EVAL_MS));
    eval_timer_->async_wait([self](const asio::error_code& ec) { self->do_eval_tick(ec); });
}

void PeerSession::do_eval_tick(const asio::error_code& ec) {
    if (ec || state_ != State::CONNECTED) return;

    auto now = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_eval_time_).count();
    if (elapsed_ms < PIPELINE_EVAL_MS) {
        // 重新调度到剩余时间
        auto self = shared_from_this();
        eval_timer_->expires_after(std::chrono::milliseconds(PIPELINE_EVAL_MS - elapsed_ms));
        eval_timer_->async_wait([self](const asio::error_code& e) { self->do_eval_tick(e); });
        return;
    }

    last_eval_time_ = now;

    // ── Fast Fail: 检查超时 Request ──
    for (auto it = pending_times_.begin(); it != pending_times_.end(); ) {
        auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();
        if (age_ms >= REQUEST_TIMEOUT_MS) {
            uint32_t ci = it->first.first;
            uint32_t begin = it->first.second;
            // 尽力发送 Cancel
            send_message(build_cancel(ci, begin, SUB_BLOCK_SIZE));
            // 通知 Scheduler 重新标记 MISSING
            if (on_request_timeout_) on_request_timeout_(ci, begin);
            it = pending_times_.erase(it);
            consecutive_timeouts_++;
            timeout_count_this_cycle_++;
        } else {
            ++it;
        }
    }

    // 连续超时断开
    if (consecutive_timeouts_ >= REQUEST_TIMEOUT_LIMIT) {
        disconnect();
        return;
    }

    // ── 动态 Pipeline 评估 ──
    pipeline_eval_cycles_++;
    if (timeout_count_this_cycle_ == 0 && avg_latency_us_ < LATENCY_THRESHOLD_US) {
        uint32_t new_cap = static_cast<uint32_t>(std::ceil(pipeline_cap_ * PIPELINE_UP_FACTOR));
        pipeline_cap_ = std::min(new_cap, PIPELINE_MAX);
    } else {
        pipeline_cap_ = std::max(pipeline_cap_ / 2, PIPELINE_MIN);
    }
    timeout_count_this_cycle_ = 0;

    // 同步到 Scheduler 的 PeerSlot
    if (scheduler_) {
        auto* slot = scheduler_->peer_slot(slot_id_);
        if (slot) {
            slot->pipeline_cap = pipeline_cap_;
            slot->consecutive_timeouts = consecutive_timeouts_;
            slot->rtt_us = static_cast<uint32_t>(avg_latency_us_);
        }
    }

    // 重新调度
    auto self = shared_from_this();
    eval_timer_->expires_after(std::chrono::milliseconds(PIPELINE_EVAL_MS));
    eval_timer_->async_wait([self](const asio::error_code& e) { self->do_eval_tick(e); });
}

void PeerSession::update_download_rate() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_choke_eval_time_).count();
    if (elapsed_s > 0.0) {
        // kbps = bytes * 8 bits/byte / 1000 / elapsed_seconds
        download_rate_kbps_ = static_cast<uint32_t>(recv_bytes_since_last_choke_ * 8.0 / elapsed_s / 1000.0);
    }
    recv_bytes_since_last_choke_ = 0;
    last_choke_eval_time_ = now;
}

} // namespace thinbt
