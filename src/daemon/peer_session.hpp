#ifndef THINBT_PEER_SESSION_HPP
#define THINBT_PEER_SESSION_HPP

#include "protocol.hpp"
#include "chunk_assembler.hpp"
#include <memory>
#include <vector>
#include <array>
#include <functional>
#include <cstdint>
#include <string>

namespace thinbt {

class PeerSession : public std::enable_shared_from_this<PeerSession> {
public:
    using OnMessage = std::function<void(std::shared_ptr<PeerSession>, P2PMsgId, const uint8_t*, uint32_t)>;
    using OnDisconnect = std::function<void(std::shared_ptr<PeerSession>)>;

    PeerSession(void* socket_ptr, const Sha1Digest& info_hash, uint32_t local_speed_mbps);
    ~PeerSession();

    void start(OnMessage on_msg, OnDisconnect on_disc);
    void send_message(const std::vector<uint8_t>& bytes);
    void disconnect();

    // State accessors
    bool is_choked() const { return am_choked_; }
    void set_choked(bool v) { am_choked_ = v; }
    bool is_interested() const { return am_interested_; }
    void set_interested(bool v) { am_interested_ = v; }
    const std::vector<bool>& remote_bitfield() const { return remote_bitfield_; }
    uint32_t link_speed_reported() const { return remote_speed_mbps_; }
    uint32_t link_speed_measured() const { return measured_speed_bps_; }
    uint32_t pending_requests() const { return pending_requests_; }
    uint32_t consecutive_timeouts() const { return consecutive_timeouts_; }
    uint32_t rtt_us() const { return rtt_us_; }
    uint32_t pipeline_cap() const { return pipeline_cap_; }
    void set_pipeline_cap(uint32_t c) { pipeline_cap_ = c; }
    void inc_pending() { pending_requests_++; }
    void dec_pending() { if (pending_requests_ > 0) pending_requests_--; }
    void inc_timeout() { consecutive_timeouts_++; }
    void reset_timeouts() { consecutive_timeouts_ = 0; }
    std::string remote_ip() const;

    // For have/bitfield handlers
    void record_have(uint32_t chunk_idx);
    void record_bitfield(const uint8_t* data, uint32_t len);

private:
    Sha1Digest our_info_hash_;
    uint32_t local_speed_mbps_;
    uint32_t remote_speed_mbps_ = 0;
    std::vector<bool> remote_bitfield_;
    std::atomic<bool> am_choked_{true};
    bool am_interested_ = false;
    uint32_t pending_requests_ = 0;
    uint32_t consecutive_timeouts_ = 0;
    uint32_t rtt_us_ = 0;
    uint32_t pipeline_cap_ = 16;
    uint32_t measured_speed_bps_ = 0;
    OnMessage on_message_;
    OnDisconnect on_disconnect_;
};

} // namespace thinbt
#endif
