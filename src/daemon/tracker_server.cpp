#include "tracker_server.hpp"
#include <algorithm>

namespace thinbt {

TrackerServer::TrackerServer(uint16_t port) : port_(port) {}
TrackerServer::~TrackerServer() = default;

std::vector<TrackerPeerEntry> TrackerServer::announce(
    const std::string& info_hash_hex, const std::string& peer_ip,
    uint16_t peer_port, uint32_t speed_mbps) {

    std::lock_guard<std::mutex> lock(mutex_);
    uint8_t flags = 0;
    if (speed_mbps >= 1000) flags |= 0x02;

    TrackerPeerEntry entry{peer_ip, peer_port, flags, std::chrono::steady_clock::now()};
    auto& swarm = swarms_[info_hash_hex];

    bool found = false;
    for (auto& p : swarm) {
        if (p.ip == peer_ip && p.port == peer_port) { p = entry; found = true; break; }
    }
    if (!found) swarm.push_back(entry);

    std::vector<TrackerPeerEntry> result;
    for (const auto& p : swarm)
        if (p.ip != peer_ip || p.port != peer_port) result.push_back(p);
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a.flags > b.flags; });
    return result;
}

void TrackerServer::cleanup_stale(uint32_t timeout_sec) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    for (auto& [hash, swarm] : swarms_) {
        swarm.erase(std::remove_if(swarm.begin(), swarm.end(),
            [&](const auto& p) {
                return std::chrono::duration_cast<std::chrono::seconds>(now - p.last_announce).count() > timeout_sec;
            }), swarm.end());
    }
}

} // namespace thinbt
