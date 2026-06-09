#include "peer_session.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "common/net_util.hpp"
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>

namespace thinbt {

PeerSession::PeerSession(asio::io_context& io, const Sha1Digest& info_hash, uint32_t local_speed_mbps)
    : socket_(std::make_shared<asio::ip::tcp::socket>(io))
    , local_speed_mbps_(local_speed_mbps)
{
    memcpy(info_hash_.data(), info_hash.data(), 20);
}

PeerSession::~PeerSession() = default;

std::string PeerSession::remote_ip() const {
    asio::error_code ec;
    auto ep = socket_->remote_endpoint(ec);
    return ec ? "unknown" : ep.address().to_string();
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
        std::vector<uint8_t> buf(length);
        ssize_t n = pread(file_fd_, buf.data(), length, static_cast<off_t>(file_off));
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
    if (len < 3) return;
    uint8_t  op    = data[0];
    uint16_t count; memcpy(&count, data + 1, 2); count = ntoh16(count);
    if (len < 3u + count * 8) return;

    for (uint16_t i = 0; i < count; i++) {
        PexPeer p;
        memcpy(&p, data + 3 + i * 8, 8);
        p.ip   = ntoh32(p.ip);
        p.port = ntoh16(p.port);
        if (!(p.flags & 0x80) && on_pex_peer_) {
            struct in_addr ia; ia.s_addr = p.ip;
            on_pex_peer_(inet_ntoa(ia), p.port, p.flags);
        }
    }
    (void)op; // 0x00=full, 0x01=delta
}

void PeerSession::handle_piece_msg(const uint8_t* data, uint32_t len) {
    if (len < 8 || !io_pool_) return;
    uint32_t index, begin;
    memcpy(&index, data, 4); index = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin = ntoh32(begin);

    PieceTask task{index, begin, len - 8, data + 8, current_body_};
    io_pool_->dispatch(task);
    dec_pending();
    if (scheduler_) scheduler_->dec_peer_pending(slot_id_);
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

} // namespace thinbt
