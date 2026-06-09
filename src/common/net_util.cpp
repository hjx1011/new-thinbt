#include "net_util.hpp"

namespace thinbt {

std::string resolve_addr(const std::string& host, uint16_t port)
{
    (void)host; (void)port;
    return "";
}

std::string get_local_ip()
{
    return "127.0.0.1";
}

int detect_link_speed_mbps()
{
    return 1000; // 默认千兆
}

std::string generate_peer_id()
{
    return std::string(20, '\0');
}

bool parse_tracker_url(const std::string&, TrackerUrl&)
{
    return false;
}

} // namespace thinbt
