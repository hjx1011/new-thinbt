#include "peer_session.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "sendfile_pool.hpp"
#include "file_read_pool.hpp"
#include "common/net_util.hpp"
#include "common/file_util.hpp"
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <thread>
#ifndef _WIN32
#include <sys/socket.h>
#endif
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace thinbt {

PeerSession::PeerSession(asio::io_context& io, const Sha1Digest& info_hash, uint32_t local_speed_mbps,
                         uint16_t local_listen_port)
    : socket_(std::make_shared<asio::ip::tcp::socket>(io))
    , local_speed_mbps_(local_speed_mbps)
    , local_listen_port_(local_listen_port)
    , eval_timer_(std::make_unique<asio::steady_timer>(io))
    , last_eval_time_(std::chrono::steady_clock::now())
    , last_download_eval_time_(std::chrono::steady_clock::now())
    , last_upload_eval_time_(std::chrono::steady_clock::now())
{
    memcpy(info_hash_.data(), info_hash.data(), 20);
    // 默认启用 sendfile，除非通过 THINBT_ENABLE_SENDFILE=0 显式关闭
    use_sendfile_ = false;
    const char* env = std::getenv("THINBT_ENABLE_SENDFILE");
    if (env && std::string(env) == "1") use_sendfile_ = true;
    // 启动全局 sendfile 线程池（幂等）
    SendfilePool::instance().start(4);
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
    h.build(info_hash_, local_speed_mbps_, local_listen_port_);
    for (int i = 0; i < 20; i++) h.peer_id[i] = static_cast<uint8_t>(rand() % 256);
    std::cerr << "[peer] send_handshake to " << remote_ip() << ":" << remote_port() << std::endl;
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
    remote_listen_port_ = h.listen_port();

    if (!handshake_sent_) send_handshake();

    state_ = State::CONNECTED;
    start_eval_timer();
    std::cerr << "[peer] eval_timer_started" << std::endl;
    start_read_header();
    std::cerr << "[peer] read_header_started" << std::endl;
    std::cerr << "[peer] handshake done with " << remote_ip() << ":" << remote_port() << " speed=" << remote_speed_mbps_ << "Mbps" << std::endl;
    if (on_handshake_done_) {
        std::cerr << "[peer] calling on_handshake_done" << std::endl;
        on_handshake_done_(shared_from_this());
        std::cerr << "[peer] on_handshake_done returned" << std::endl;
    }
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
    case P2PMsgId::CHOKE:
        peer_choking_me_.store(true, std::memory_order_release);
        if (scheduler_) scheduler_->on_choke_change(slot_id_, true);
        break;
    case P2PMsgId::UNCHOKE:
        peer_choking_me_.store(false, std::memory_order_release);
        if (scheduler_) {
            scheduler_->on_choke_change(slot_id_, false);
            if (scheduler_->missing_count() > 0)
                send_interested();
        }
        break;
    case P2PMsgId::INTERESTED:
        peer_interested_.store(true, std::memory_order_release);
        if (choking_peer_.load(std::memory_order_acquire)) {
            set_choking_peer(false);
            send_message(build_message(P2PMsgId::UNCHOKE, nullptr, 0));
        }
        break;
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
    if (scheduler_ && scheduler_->wants_peer(slot_id_)) send_interested();
}

void PeerSession::handle_bitfield_msg(const uint8_t* data, uint32_t len) {
    record_bitfield(data, len);
    if (scheduler_) scheduler_->on_bitfield(slot_id_, remote_bitfield_);
    if (scheduler_ && scheduler_->wants_peer(slot_id_)) send_interested();
}

void PeerSession::handle_request_msg(const uint8_t* data) {
    if (choking_peer_.load(std::memory_order_acquire)) return;

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
            // 如果启用 sendfile，并且写队列为空且当前没有异步写进行，则采用后台线程发送 header+sendfile
            bool try_sendfile = use_sendfile_;
            {
                std::lock_guard<std::mutex> lk(write_mtx_);
                if (!try_sendfile || !write_queue_.empty() || is_writing_) try_sendfile = false;
                else is_writing_ = true; // 保证我们在后台发送期间不被 do_write 干扰
            }

            if (try_sendfile) {
                auto self = shared_from_this();
                int local_file_fd = file_fd_;
                uint64_t off = file_off;
                uint32_t send_len = length;
                uint32_t idx = index;
                uint32_t bg = begin;
                // 使用全局 sendfile 线程池提交任务，避免频繁创建线程
                SendfilePool::instance().post([self, local_file_fd, off, send_len, idx, bg]() mutable {
                    // 检查连接是否已关闭，防止在已关闭的 socket 上操作
                    if (self->closed_.load(std::memory_order_acquire)) return;
                    auto sockfd = self->socket_->native_handle();
                    // 构建 13 字节 header: [len_be:4][id:1][idx:4][beg:4]
                    uint32_t msg_len = 1 + 8 + send_len;
                    uint32_t len_be = htonl(msg_len);
                    uint8_t hdr[13];
                    memcpy(hdr, &len_be, 4);
                    hdr[4] = static_cast<uint8_t>(P2PMsgId::PIECE);
                    uint32_t idx_be = htonl(idx), beg_be = htonl(bg);
                    memcpy(hdr + 5, &idx_be, 4);
                    memcpy(hdr + 9, &beg_be, 4);

                    // 先发送 header（MSG_NOSIGNAL 防止对端断开时 SIGPIPE 杀死进程）
                    ssize_t hn = ::send(sockfd, reinterpret_cast<const char*>(hdr), sizeof(hdr), MSG_NOSIGNAL);
                    ssize_t sent_body = 0;
                    if (hn == static_cast<ssize_t>(sizeof(hdr))) {
                        // sendfile 发送数据体
                        uint64_t off_var = off;
                        ssize_t n = sendfile_zero_copy(sockfd, local_file_fd, off_var, send_len);
                        if (n > 0) sent_body = n;
                    }

                    // 回到 io_context 线程更新状态并可能触发后续写
                    asio::post(self->socket_->get_executor(), [self, sent_header = (ssize_t)13, sent_body]() {
                        if (sent_body > 0) self->add_sent_bytes(static_cast<uint32_t>(sent_header + sent_body));
                        // 完成 sendfile 后释放写锁并触发后续写
                        bool has_more = false;
                        {
                            std::lock_guard<std::mutex> lk(self->write_mtx_);
                            self->is_writing_ = false;
                            has_more = !self->write_queue_.empty();
                        }
                        if (has_more) self->do_write();
                    });
                });
            } else {
                // 将阻塞的 pread 转到后台线程池执行，读取完成后回到 io_context 发送
                {
                    auto self = shared_from_this();
                    int fd = file_fd_;
                    off_t off = static_cast<off_t>(file_off);
                    size_t len = length;
                    uint32_t idx = index;
                    uint32_t bg = begin;
                    // 启动文件读线程池（幂等）
                    FileReadPool::instance().start(2);
                    FileReadPool::instance().post_read(fd, off, len,
                        [self, idx, bg](ssize_t n, std::shared_ptr<std::vector<uint8_t>> data) {
                            // 检查连接是否已关闭
                            if (self->closed_.load(std::memory_order_acquire)) return;
                            // 在 pool 线程调用的回调里，把实际发送调度回 io_context
                            asio::post(self->socket_->get_executor(), [self, n, data, idx, bg]() {
                                if (n > 0 && data && !data->empty()) {
                                    auto msg = build_piece(idx, bg, data->data(), static_cast<uint32_t>(n));
                                    self->add_sent_bytes(msg.size());
                                    self->send_message(std::move(msg));
                                }
                            });
                        });
                }
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
        bool is_left = (p.flags & 0x80) != 0;

        if (op == 0x01 && is_left) {
            // Delta: 对端离开，通知 PeerManager 从本地池移除
            if (on_pex_remove_) {
                struct in_addr ia; ia.s_addr = p.ip;
                on_pex_remove_(inet_ntoa(ia), ntoh16(p.port));
            }
        } else if (!is_left && on_pex_peer_) {
            // 全量: 忽略 left Peer；增量: 新加入的 Peer 尝试连接
            struct in_addr ia; ia.s_addr = p.ip;
            on_pex_peer_(inet_ntoa(ia), ntoh16(p.port), p.flags);
        }
    }
}

void PeerSession::handle_piece_msg(const uint8_t* data, uint32_t len) {
    if (len < 8 || !io_pool_) return;
    uint32_t index, begin;
    memcpy(&index, data, 4); index = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin = ntoh32(begin);

    // 追踪：记录每个 sub-block 来自哪个 peer
    std::cerr << "[rx] chunk=" << index << " sub_begin=" << begin
              << " len=" << (len - 8) << " from=" << remote_ip() << std::endl;

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
    std::shared_ptr<std::vector<uint8_t>> owned;
    {
        std::lock_guard<std::mutex> lock(write_mtx_);
        if (write_queue_.empty()) { is_writing_ = false; return; }
        owned = std::make_shared<std::vector<uint8_t>>(std::move(write_queue_.front()));
        write_queue_.pop_front();
    }

    auto buf = asio::buffer(*owned);
    asio::async_write(*socket_, buf,
        [self, owned](asio::error_code ec, size_t) {
            if (ec) { self->disconnect(); return; }
            bool has_more = false;
            {
                std::lock_guard<std::mutex> lk(self->write_mtx_);
                has_more = !self->write_queue_.empty();
                if (!has_more) self->is_writing_ = false;
            }
            if (has_more) self->do_write();
        });
}

void PeerSession::disconnect() {
    if (state_ == State::DISCONNECTED) return;
    state_ = State::DISCONNECTED;
    closed_.store(true, std::memory_order_release);  // 先标记关闭，阻止后台线程访问 socket
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
    auto elapsed_s = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_download_eval_time_).count();
    if (elapsed_s > 0.0) {
        // kbps = bytes * 8 bits/byte / 1000 / elapsed_seconds
        download_rate_kbps_ = static_cast<uint32_t>(recv_bytes_since_last_choke_ * 8.0 / elapsed_s / 1000.0);
    }
    recv_bytes_since_last_choke_ = 0;
    last_download_eval_time_ = now;
}

void PeerSession::update_upload_rate() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_upload_eval_time_).count();
    if (elapsed_s > 0.0) {
        upload_rate_kbps_ = static_cast<uint32_t>(sent_bytes_since_last_choke_ * 8.0 / elapsed_s / 1000.0);
    }
    sent_bytes_since_last_choke_ = 0;
    last_upload_eval_time_ = now;
}

} // namespace thinbt
