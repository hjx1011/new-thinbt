#ifndef THINBT_TRACKER_SERVER_HPP
#define THINBT_TRACKER_SERVER_HPP

#include "common/platform.hpp"
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <chrono>
#include <cstdint>

namespace thinbt {

struct TrackerPeerEntry {
    std::string ip;
    uint16_t port;
    uint8_t  flags;
    std::chrono::steady_clock::time_point last_announce;
};

class TrackerServer {
public:
    TrackerServer(uint16_t port = 8080);
    ~TrackerServer();

    std::vector<TrackerPeerEntry> announce(const std::string& info_hash_hex,
                                            const std::string& peer_ip,
                                            uint16_t peer_port,
                                            uint32_t speed_mbps);
    void cleanup_stale(uint32_t timeout_sec = 90);
    uint16_t port() const { return port_; }

private:
    uint16_t port_;
    std::mutex mutex_;
    std::map<std::string, std::vector<TrackerPeerEntry>> swarms_;
};

} // namespace thinbt
#endif
