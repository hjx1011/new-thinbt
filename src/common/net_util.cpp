#include "net_util.hpp"

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <random>

#ifdef _WIN32
    #include <iphlpapi.h>
    #ifdef _MSC_VER
        #pragma comment(lib, "Iphlpapi.lib")
    #endif
#else
    #include <ifaddrs.h>
    #include <netdb.h>
    #include <net/if.h>
    #include <sys/socket.h>
    #include <fstream>
#endif

namespace thinbt {

bool resolve_addr(const std::string& host, uint16_t port, struct sockaddr_in& addr) {
    struct addrinfo hints;
    struct addrinfo* res = nullptr;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%u", port);

    int ret = getaddrinfo(host.c_str(), port_str, &hints, &res);
    if (ret != 0) {
        fprintf(stderr, "resolve_addr: getaddrinfo failed for %s:%u: %s\n",
                host.c_str(), port, gai_strerror(ret));
        return false;
    }

    if (res == nullptr || res->ai_addr == nullptr) {
        fprintf(stderr, "resolve_addr: no address found for %s:%u\n",
                host.c_str(), port);
        if (res) freeaddrinfo(res);
        return false;
    }

    memcpy(&addr, res->ai_addr, sizeof(struct sockaddr_in));
    freeaddrinfo(res);
    return true;
}

std::string get_local_ip() {
#ifdef _WIN32
    ULONG buf_len = 15000;
    std::vector<BYTE> buf(buf_len);
    PIP_ADAPTER_ADDRESSES adapters = reinterpret_cast<PIP_ADAPTER_ADDRESSES>(buf.data());

    ULONG ret = GetAdaptersAddresses(AF_INET,
        GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER,
        nullptr, adapters, &buf_len);

    if (ret == ERROR_BUFFER_OVERFLOW) {
        buf.resize(buf_len);
        adapters = reinterpret_cast<PIP_ADAPTER_ADDRESSES>(buf.data());
        ret = GetAdaptersAddresses(AF_INET,
            GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER,
            nullptr, adapters, &buf_len);
    }

    if (ret != NO_ERROR) {
        fprintf(stderr, "get_local_ip: GetAdaptersAddresses failed (code=%lu)\n",
                static_cast<unsigned long>(ret));
        return "";
    }

    for (PIP_ADAPTER_ADDRESSES a = adapters; a != nullptr; a = a->Next) {
        if (a->IfType == IF_TYPE_SOFTWARE_LOOPBACK) continue;
        if (a->OperStatus != IfOperStatusUp) continue;

        for (PIP_ADAPTER_UNICAST_ADDRESS ua = a->FirstUnicastAddress; ua != nullptr; ua = ua->Next) {
            if (ua->Address.lpSockaddr->sa_family == AF_INET) {
                char ip[INET_ADDRSTRLEN];
                struct sockaddr_in* sin = reinterpret_cast<struct sockaddr_in*>(ua->Address.lpSockaddr);
                inet_ntop(AF_INET, &sin->sin_addr, ip, sizeof(ip));
                return std::string(ip);
            }
        }
    }
    return "";
#else
    struct ifaddrs* ifap = nullptr;
    if (getifaddrs(&ifap) != 0) {
        perror("get_local_ip: getifaddrs");
        return "";
    }

    std::string result;
    for (struct ifaddrs* ifa = ifap; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        if (ifa->ifa_flags & IFF_LOOPBACK) continue;
        if (!(ifa->ifa_flags & IFF_UP)) continue;

        char ip[INET_ADDRSTRLEN];
        struct sockaddr_in* sin = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
        inet_ntop(AF_INET, &sin->sin_addr, ip, sizeof(ip));
        result = std::string(ip);
        break;
    }

    freeifaddrs(ifap);
    return result;
#endif
}

uint32_t detect_link_speed_mbps() {
#ifdef _WIN32
    ULONG buf_len = 0;
    if (GetIfTable(nullptr, &buf_len, FALSE) != ERROR_INSUFFICIENT_BUFFER) {
        fputs("detect_link_speed: GetIfTable size query failed\n", stderr);
        return 1000;
    }

    std::vector<BYTE> buf(buf_len);
    PMIB_IFTABLE if_table = reinterpret_cast<PMIB_IFTABLE>(buf.data());

    if (GetIfTable(if_table, &buf_len, FALSE) != NO_ERROR) {
        fputs("detect_link_speed: GetIfTable failed\n", stderr);
        return 1000;
    }

    for (DWORD i = 0; i < if_table->dwNumEntries; i++) {
        MIB_IFROW& row = if_table->table[i];
        if (row.dwType == MIB_IF_TYPE_LOOPBACK) continue;
        if (row.dwOperStatus < IF_OPER_STATUS_CONNECTED) continue;

        uint32_t mbps = static_cast<uint32_t>(row.dwSpeed / 1000000);
        if (mbps > 0) return mbps;
    }
    return 1000;
#else
    // 获取活跃接口名，复用 getifaddrs 查找第一个非 loopback 的 IPv4 接口
    struct ifaddrs* ifap = nullptr;
    if (getifaddrs(&ifap) != 0) {
        perror("detect_link_speed: getifaddrs");
        return 1000;
    }

    std::string ifname;
    for (struct ifaddrs* ifa = ifap; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        if (ifa->ifa_flags & IFF_LOOPBACK) continue;
        if (!(ifa->ifa_flags & IFF_UP)) continue;

        ifname = ifa->ifa_name;
        break;
    }
    freeifaddrs(ifap);

    if (ifname.empty()) return 1000;

    // 读取内核导出的网卡协商速率（非实测速率，是握手上报的链路能力）
    std::string path = "/sys/class/net/" + ifname + "/speed";
    std::ifstream speed_file(path);
    if (!speed_file.is_open()) {
        fprintf(stderr, "detect_link_speed: cannot open %s\n", path.c_str());
        return 1000;
    }

    uint32_t speed = 0;
    if (!(speed_file >> speed) || speed == 0) {
        fprintf(stderr, "detect_link_speed: failed to parse speed from %s\n", path.c_str());
        return 1000;
    }

    return speed;
#endif
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
    const char* prefix = "thinbt://";
    const size_t prefix_len = strlen(prefix);

    if (url.compare(0, prefix_len, prefix) != 0) {
        fprintf(stderr, "parse_tracker_url: missing thinbt:// prefix in '%s'\n", url.c_str());
        return false;
    }

    const char* p = url.c_str() + prefix_len;
    std::string host;

    // IPv6 格式: thinbt://[addr]:port/...
    if (*p == '[') {
        p++;
        const char* bracket_end = strchr(p, ']');
        if (bracket_end == nullptr) {
            fputs("parse_tracker_url: unmatched '[' in IPv6 address\n", stderr);
            return false;
        }
        host.assign(p, static_cast<size_t>(bracket_end - p));
        p = bracket_end + 1;
    } else {
        // IPv4 / hostname: 取下一个 ':' 或 '/' 之前的部分
        const char* colon = strchr(p, ':');
        const char* slash = strchr(p, '/');
        const char* host_end = p + strlen(p);

        if (colon != nullptr && colon < host_end) host_end = colon;
        if (slash != nullptr && slash < host_end) host_end = slash;

        if (host_end == p) {
            fputs("parse_tracker_url: empty host\n", stderr);
            return false;
        }
        host.assign(p, static_cast<size_t>(host_end - p));
        p = host_end;
    }

    // 端口
    if (*p != ':') {
        fprintf(stderr, "parse_tracker_url: missing port after host in '%s'\n", url.c_str());
        return false;
    }
    p++;

    char* end = nullptr;
    long port = strtol(p, &end, 10);
    if (end == p || port < 1 || port > 65535) {
        fprintf(stderr, "parse_tracker_url: invalid port in '%s'\n", url.c_str());
        return false;
    }

    result.host = host;
    result.port = static_cast<uint16_t>(port);
    return true;
}

} // namespace thinbt
