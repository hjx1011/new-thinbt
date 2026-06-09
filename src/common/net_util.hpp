#ifndef THINBT_NET_UTIL_HPP
#define THINBT_NET_UTIL_HPP

#include "platform.hpp"
#include <string>
#include <vector>
#include <cstdint>

namespace thinbt {

struct TrackerUrl {
    std::string host;
    uint16_t port = 8080;
};

bool resolve_addr(const std::string& host, uint16_t port, struct sockaddr_in& addr);
std::string get_local_ip();
uint32_t detect_link_speed_mbps();
std::vector<uint8_t> generate_peer_id();
bool parse_tracker_url(const std::string& url, TrackerUrl& result);

} // namespace thinbt
#endif
