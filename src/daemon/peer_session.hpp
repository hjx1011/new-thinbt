#ifndef THINBT_PEER_SESSION_HPP
#define THINBT_PEER_SESSION_HPP

#include "protocol.hpp"
#include "chunk_assembler.hpp"
#include <asio.hpp>
#include <memory>
#include <deque>
#include <mutex>
#include <vector>
#include <array>
#include <functional>
#include <cstdint>

namespace thinbt {

class Scheduler;
class IOWorkerPool;

class PeerSession : public std::enable_shared_from_this<PeerSession> {
public:
    using OnDisconnect = std::function<void(std::shared_ptr<PeerSession>)>;

    PeerSession(asio::io_context& io, const Sha1Digest& info_hash, uint32_t local_speed_mbps);
    ~PeerSession();

    // Inbound: peer connected to us
    void start_inbound(asio::ip::tcp::socket sock, OnDisconnect on_disc);

    // Outbound: we connect to peer
    void start_outbound(const std::string& host, uint16_t port, OnDisconnect on_disc);

    void send_message(std::vector<uint8_t> buf);
    void disconnect();

    // Accessors
    asio::ip::tcp::socket& socket() { return *socket_; }
    bool is_choked() const { return am_choked_.load(std::memory_order_acquire); }
    void set_choked(bool v) { am_choked_.store(v, std::memory_order_release); }
    const std::vector<bool>& remote_bitfield() const { return remote_bitfield_; }
    uint32_t link_speed_reported() const { return remote_speed_mbps_; }
    uint32_t pipeline_cap() const { return pipeline_cap_; }
    void set_pipeline_cap(uint32_t c) { pipeline_cap_ = c; }
    uint32_t pending_requests() const { return pending_requests_; }
    void inc_pending() { pending_requests_++; }
    void dec_pending() { if (pending_requests_ > 0) pending_requests_--; }
    uint32_t slot_id() const { return slot_id_; }
    void set_slot_id(uint32_t id) { slot_id_ = id; }
    std::string remote_ip() const;

    void record_have(uint32_t chunk_idx);
    void record_bitfield(const uint8_t* data, uint32_t len);

    void set_scheduler(Scheduler* s) { scheduler_ = s; }
    void set_io_pool(IOWorkerPool* p) { io_pool_ = p; }
    void set_file_fd(int fd) { file_fd_ = fd; }

private:
    enum State { HANDSHAKE, CONNECTED, DISCONNECTED };

    void send_handshake();
    void start_read_handshake();
    void handle_handshake(const asio::error_code& ec, size_t);

    void start_read_header();
    void start_read_body(uint32_t body_len, P2PMsgId msg_id);
    void dispatch_message(P2PMsgId id, const uint8_t* data, uint32_t len);

    void handle_have_msg(const uint8_t* data);
    void handle_bitfield_msg(const uint8_t* data, uint32_t len);
    void handle_request_msg(const uint8_t* data);
    void handle_piece_msg(const uint8_t* data, uint32_t len);
    void handle_pex_msg(const uint8_t* data, uint32_t len);

    void do_write();

    std::shared_ptr<asio::ip::tcp::socket> socket_;
    Sha1Digest info_hash_;
    uint32_t local_speed_mbps_;
    State state_ = HANDSHAKE;
    OnDisconnect on_disconnect_;

    std::array<uint8_t, 67> handshake_buf_{};
    bool handshake_sent_ = false;

    std::array<uint8_t, 5> header_buf_{};

    uint32_t remote_speed_mbps_ = 0;
    std::vector<bool> remote_bitfield_;
    std::atomic<bool> am_choked_{true};

    std::deque<std::vector<uint8_t>> write_queue_;
    std::mutex write_mtx_;
    bool is_writing_ = false;

    uint32_t pending_requests_ = 0;
    uint32_t pipeline_cap_ = 16;
    uint32_t slot_id_ = 0;

    Scheduler* scheduler_ = nullptr;
    IOWorkerPool* io_pool_ = nullptr;
    int file_fd_ = -1;

    static constexpr uint32_t MAX_MSG_SIZE = 17600;
};

} // namespace thinbt
#endif
