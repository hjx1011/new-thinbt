#include "tracker_client.hpp"

namespace thinbt {

void TrackerClient::announce(const std::string& /*tracker_host*/, uint16_t /*tracker_port*/,
                              const std::string& /*info_hash_hex*/, uint16_t /*p2p_port*/,
                              uint32_t /*speed_mbps*/, OnPeers /*on_peers*/, OnError /*on_error*/) {
    // Stub — full Asio implementation in M3 integration
}

} // namespace thinbt
