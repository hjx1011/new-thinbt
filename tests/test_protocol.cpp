#include "daemon/protocol.hpp"

#include <cstdint>
#include <cstring>
#include <iostream>

using namespace thinbt;

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

int main() {
    if (sizeof(Handshake) != 67) return fail("Handshake must be 67 bytes");
    if (sizeof(PexPeer) != 8) return fail("PexPeer must be 8 bytes");

    Sha1Digest ih{};
    Handshake hs{};
    hs.build(ih, 1000, 17889);
    auto hs_buf = serialize_handshake(hs);
    Handshake parsed{};
    if (!parse_handshake(hs_buf.data(), hs_buf.size(), parsed)) {
        return fail("handshake with listen port should parse");
    }
    if (parsed.listen_port() != 17889) {
        return fail("handshake must preserve the peer listening port for PEX");
    }

    PexPeer peer{};
    peer.ip = inet_addr("192.168.1.10");
    peer.port = htons(16889);
    peer.flags = 0x02;

    auto msg = build_pex(false, {peer});
    if (msg.size() != 16) return fail("PEX message size must be 16 bytes for one peer");

    uint32_t msg_len = 0;
    std::memcpy(&msg_len, msg.data(), 4);
    if (ntoh32(msg_len) != 12) return fail("PEX message length prefix must be 12");
    if (msg[4] != static_cast<uint8_t>(P2PMsgId::PEX)) return fail("PEX message id mismatch");
    if (msg[5] != 0x00) return fail("PEX full update op must be 0x00");

    uint16_t count = 0;
    std::memcpy(&count, msg.data() + 6, 2);
    if (ntoh16(count) != 1) return fail("PEX count must be 1");

    PexPeer encoded{};
    std::memcpy(&encoded, msg.data() + 8, sizeof(encoded));
    if (encoded.ip != peer.ip) return fail("PEX ip must already be network byte order");
    if (encoded.port != peer.port) return fail("PEX port must already be network byte order");
    if (encoded.flags != peer.flags) return fail("PEX flags mismatch");

    PexPeer custom_port{};
    custom_port.ip = inet_addr("10.0.0.8");
    custom_port.port = htons(23456);
    custom_port.flags = 0x80;
    auto delta = build_pex(true, {custom_port});
    PexPeer delta_encoded{};
    std::memcpy(&delta_encoded, delta.data() + 8, sizeof(delta_encoded));
    if (delta_encoded.port != htons(23456)) return fail("PEX delta must preserve configured peer port");

    std::cout << "Protocol tests passed!" << std::endl;
    return 0;
}
