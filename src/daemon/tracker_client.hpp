#ifndef THINBT_TRACKER_CLIENT_HPP
#define THINBT_TRACKER_CLIENT_HPP

#include "protocol.hpp"
#include <string>
#include <vector>
#include <functional>

namespace thinbt {

class TrackerClient {
public:
    using OnPeers = std::function<void(const std::vector<PexPeer>& peers)>;
    using OnError = std::function<void(const std::string& error)>;

    TrackerClient() = default;

    void announce(const std::string& tracker_host, uint16_t tracker_port,
                  const std::string& info_hash_hex, uint16_t p2p_port,
                  uint32_t speed_mbps, OnPeers on_peers, OnError on_error);
};

} // namespace thinbt
#endif
