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
#include <set>
#include <map>
#include <chrono>

namespace thinbt {

class Scheduler;
class IOWorkerPool;

class PeerSession : public std::enable_shared_from_this<PeerSession> {
public:
    using OnDisconnect = std::function<void(std::shared_ptr<PeerSession>)>;
    using OnHandshakeDone = std::function<void(std::shared_ptr<PeerSession>)>;
    using OnPexPeer   = std::function<void(const std::string& ip, uint16_t port, uint8_t flags)>;
    using OnPexRemove = std::function<void(const std::string& ip, uint16_t port)>;

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
    uint16_t remote_port() const;

    void record_have(uint32_t chunk_idx);
    void record_bitfield(const uint8_t* data, uint32_t len);

    void set_scheduler(Scheduler* s) { scheduler_ = s; }
    void set_io_pool(IOWorkerPool* p) { io_pool_ = p; }
    void set_file_fd(int fd) { file_fd_ = fd; }
    void set_chunk_offsets(const std::vector<uint64_t>* offsets) { chunk_offsets_ = offsets; }
    void set_on_pex_peer(OnPexPeer cb) { on_pex_peer_ = std::move(cb); }
    void set_on_pex_remove(OnPexRemove cb) { on_pex_remove_ = std::move(cb); }
    void set_on_handshake_done(OnHandshakeDone cb) { on_handshake_done_ = std::move(cb); }

    bool is_peer_interested() const { return peer_interested_.load(std::memory_order_acquire); }
    bool am_interested() const { return am_interested_.load(std::memory_order_acquire); }
    void send_interested();
    void send_not_interested();

    // Fast Fail + Pipeline
    using OnRequestTimeout = std::function<void(uint32_t chunk_idx, uint32_t begin)>;
    void set_on_request_timeout(OnRequestTimeout cb) { on_request_timeout_ = std::move(cb); }
    uint32_t consecutive_timeouts() const { return consecutive_timeouts_; }
    void reset_timeouts() { consecutive_timeouts_ = 0; }
    void record_request_sent(uint32_t chunk_idx, uint32_t begin);
    void start_eval_timer();
    uint32_t download_rate_kbps() const { return download_rate_kbps_; }
    void update_download_rate();
    uint32_t upload_rate_kbps() const { return upload_rate_kbps_; }
    void update_upload_rate();
    void add_sent_bytes(uint32_t n) { sent_bytes_since_last_choke_ += n; }

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
    void handle_cancel_msg(const uint8_t* data);
    void handle_pex_msg(const uint8_t* data, uint32_t len);

    std::set<std::pair<uint32_t, uint32_t>> cancelled_set_; // {chunk_idx, begin}

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
    std::atomic<bool> peer_interested_{false};
    std::atomic<bool> am_interested_{false};

    std::deque<std::vector<uint8_t>> write_queue_;
    std::mutex write_mtx_;
    bool is_writing_ = false;

    uint32_t pending_requests_ = 0;
    uint32_t pipeline_cap_ = 16;
    uint32_t slot_id_ = 0;

    Scheduler* scheduler_ = nullptr;
    IOWorkerPool* io_pool_ = nullptr;
    int file_fd_ = -1;
    const std::vector<uint64_t>* chunk_offsets_ = nullptr;
    OnPexPeer on_pex_peer_;
    OnPexRemove on_pex_remove_;
    OnHandshakeDone on_handshake_done_;
    OnRequestTimeout on_request_timeout_;
    std::shared_ptr<std::vector<uint8_t>> current_body_;

    // Fast Fail: 记录每个 Request 的发出时间 {chunk_idx, begin} → time_point
    std::map<std::pair<uint32_t, uint32_t>, std::chrono::steady_clock::time_point> pending_times_;
    uint32_t consecutive_timeouts_ = 0;
    std::unique_ptr<asio::steady_timer> eval_timer_;

    // 动态 Pipeline 评估
    uint32_t pipeline_eval_cycles_ = 0;
    uint32_t timeout_count_this_cycle_ = 0;
    double avg_latency_us_ = 500.0; // 初始假设 500μs
    std::chrono::steady_clock::time_point last_eval_time_;

    // Choke 排序：实际下载/上传速率
    uint64_t recv_bytes_since_last_choke_ = 0;
    uint64_t sent_bytes_since_last_choke_ = 0;
    uint32_t download_rate_kbps_ = 0;
    uint32_t upload_rate_kbps_ = 0;
    std::chrono::steady_clock::time_point last_choke_eval_time_;

    void do_eval_tick(const asio::error_code& ec);

    static constexpr uint32_t MAX_MSG_SIZE = 65536;  // 64KB，支持 524288 chunk 的 bitfield
    static constexpr uint32_t PIPELINE_EVAL_MS = 500;
    static constexpr uint32_t REQUEST_TIMEOUT_MS = 3000;
    static constexpr uint32_t REQUEST_TIMEOUT_LIMIT = 3;
    static constexpr uint32_t PIPELINE_MIN = 8;
    static constexpr uint32_t PIPELINE_MAX = 100;
    static constexpr double LATENCY_THRESHOLD_US = 2000.0;
    static constexpr double PIPELINE_UP_FACTOR = 1.5;
    static constexpr double LATENCY_EMA_ALPHA = 0.3;
};

} // namespace thinbt
#endif
