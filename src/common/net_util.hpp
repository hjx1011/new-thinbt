#ifndef THINBT_NET_UTIL_HPP
#define THINBT_NET_UTIL_HPP

#include <string>
#include <vector>

namespace thinbt {

struct TrackerUrl {
    std::string protocol;   // "http" or "https"
    std::string host;
    uint16_t    port = 0;
    std::string path;
    std::string info_hash_hex;
};

std::string resolve_addr(const std::string& host, uint16_t port);
std::string get_local_ip();

// 检测本机网络链路速率（Mbps），千兆=1000，百兆=100
int detect_link_speed_mbps();

// 生成 20 字节随机 peer_id
std::string generate_peer_id();

// 解析 tracker URL，提取 host/port/path/info_hash
bool parse_tracker_url(const std::string& url, TrackerUrl& out);

} // namespace thinbt

#endif
