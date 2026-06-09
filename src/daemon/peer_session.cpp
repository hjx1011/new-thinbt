#include "peer_session.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "common/net_util.hpp"
#include <cstring>
#include <cstdlib>
#include <iostream>

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
            self->dispatch_message(msg_id, body->data(), static_cast<uint32_t>(body->size()));
            self->start_read_header();
        });
}

void PeerSession::dispatch_message(P2PMsgId id, const uint8_t* data, uint32_t len) {
    switch (id) {
    case P2PMsgId::CHOKE:          am_choked_.store(true, std::memory_order_release);  break;
    case P2PMsgId::UNCHOKE:        am_choked_.store(false, std::memory_order_release); break;
    case P2PMsgId::INTERESTED:     /* peer is interested in our data */ break;
    case P2PMsgId::NOT_INTERESTED: /* peer not interested */           break;
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

    uint64_t file_off = index * 131072ULL + begin; // 128KB avg chunk
#ifdef __linux__
    if (file_fd_ >= 0) {
        off_t off = static_cast<off_t>(file_off);
        ::sendfile(socket_->native_handle(), file_fd_, &off, length);
    }
#endif
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
        // If not "left" marker, try connecting
        if (!(p.flags & 0x80) && scheduler_) {
            struct in_addr ia; ia.s_addr = p.ip;
            // PeerManager callback to connect_to would be set up via scheduler
            // For now, parsed but connection deferred to PeerManager
        }
    }
    (void)op; // 0x00=full, 0x01=delta
}

void PeerSession::handle_piece_msg(const uint8_t* data, uint32_t len) {
    if (len < 8 || !io_pool_) return;
    uint32_t index, begin;
    memcpy(&index, data, 4); index = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin = ntoh32(begin);

    PieceTask task{index, begin, len - 8, data + 8, nullptr};
    io_pool_->dispatch(task);
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
    asio::async_write(*socket_, asio::buffer(front),
        [self, front = std::move(front)](asio::error_code ec, size_t) mutable {
            if (ec) { self->disconnect(); return; }
            std::lock_guard<std::mutex> lk(self->write_mtx_);
            self->write_queue_.pop_front();
            if (!self->write_queue_.empty())
                self->do_write();
            else
                self->is_writing_ = false;
        });
}

void PeerSession::disconnect() {
    if (state_ == State::DISCONNECTED) return;
    state_ = State::DISCONNECTED;
    asio::error_code ec;
    socket_->close(ec);
    if (on_disconnect_) on_disconnect_(shared_from_this());
}

} // namespace thinbt
