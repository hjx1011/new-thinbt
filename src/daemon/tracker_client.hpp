#ifndef THINBT_TRACKER_CLIENT_HPP
#define THINBT_TRACKER_CLIENT_HPP

#include "protocol.hpp"
#include <asio.hpp>
#include <string>
#include <vector>
#include <functional>
#include <memory>

namespace thinbt {

class TrackerClient : public std::enable_shared_from_this<TrackerClient> {
public:
    using OnPeers = std::function<void(const std::vector<PexPeer>&)>;
    using OnDead  = std::function<void()>;

    TrackerClient(asio::io_context& io, const std::string& info_hash_hex,
                  uint16_t p2p_port, uint32_t speed_mbps);

    void announce(const std::string& host, uint16_t port, OnPeers on_peers);
    void set_retry_enabled(bool v) { retry_enabled_ = v; }
    void set_on_dead(OnDead cb) { on_dead_ = std::move(cb); }

private:
    void do_announce(std::string host, uint16_t port, OnPeers on_peers);
    void schedule_retry(OnPeers on_peers, std::string host, uint16_t port);

    asio::io_context& io_;
    std::string info_hash_hex_;
    uint16_t p2p_port_;
    uint32_t speed_mbps_;
    bool retry_enabled_ = true;
    uint32_t retry_count_ = 0;
    OnDead on_dead_;
    bool dead_signalled_ = false;
    static constexpr uint32_t MAX_RETRIES = 240;
};

} // namespace thinbt
#endif
