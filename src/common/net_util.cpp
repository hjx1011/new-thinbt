#include "net_util.hpp"

#include <cstring>
#include <random>

namespace thinbt {

bool resolve_addr(const std::string& host, uint16_t port, struct sockaddr_in& addr) {
    (void)host; (void)port;
    memset(&addr, 0, sizeof(addr));
    return false;
}

std::string get_local_ip() {
    return "127.0.0.1";
}

uint32_t detect_link_speed_mbps() {
    return 1000;
}

std::vector<uint8_t> generate_peer_id() {
    std::vector<uint8_t> id(20);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);
    for (int i = 0; i < 20; i++) id[i] = static_cast<uint8_t>(dist(gen));
    return id;
}

bool parse_tracker_url(const std::string& url, TrackerUrl& result) {
    (void)url; (void)result;
    return false;
}

} // namespace thinbt
