#include "daemon/peer_manager.hpp"
#include "daemon/protocol.hpp"

#include <iostream>
#include <vector>

using namespace thinbt;

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

int main() {
    if (is_connectable_peer_ip("unknown")) {
        return fail("'unknown' must not be treated as a connectable peer IP");
    }
    if (is_connectable_peer_ip("")) {
        return fail("empty peer IP must not be connectable");
    }
    if (is_connectable_peer_ip("0.0.0.0")) {
        return fail("0.0.0.0 must not be connectable");
    }
    if (is_connectable_peer_ip("255.255.255.255")) {
        return fail("broadcast peer IP must not be connectable");
    }
    if (!is_connectable_peer_ip("192.168.177.56")) {
        return fail("valid IPv4 peer IP should be connectable");
    }
    std::vector<std::string> local_ips{"127.0.0.1", "192.168.177.56"};
    if (!is_self_peer_endpoint(local_ips, 16889, "192.168.177.56", 16889)) {
        return fail("peer manager must recognize the local LAN address as self");
    }
    if (!is_self_peer_endpoint(local_ips, 16889, "127.0.0.1", 16889)) {
        return fail("peer manager must recognize loopback as self");
    }
    if (is_self_peer_endpoint(local_ips, 16889, "192.168.177.177", 16889)) {
        return fail("remote peers on the same port must not be treated as self");
    }
    if (is_self_peer_endpoint(local_ips, 16889, "192.168.177.56", 22345)) {
        return fail("same host on a different port must not be treated as the local p2p endpoint");
    }

    PexPeer advertised{};
    advertised.ip = inet_addr("192.168.177.177");
    advertised.port = htons(17889);
    advertised.flags = 0x02;
    auto pex = build_pex(false, {advertised});
    if (pex.size() < 16) {
        return fail("pex payload should include an advertised peer endpoint");
    }
    PexPeer encoded{};
    memcpy(&encoded, pex.data() + 8, sizeof(PexPeer));
    if (ntoh16(encoded.port) != 17889) {
        return fail("pex must preserve each peer's real listening port instead of overwriting it with the sender port");
    }

    std::cout << "Peer manager tests passed!" << std::endl;
    return 0;
}
