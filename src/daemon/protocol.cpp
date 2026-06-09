#include "protocol.hpp"
#include <cstring>
#include <algorithm>

namespace thinbt {

void Handshake::build(const Sha1Digest& ih, uint32_t speed) {
    memset(protocol_id, 0, 19);
    memcpy(protocol_id, PROTOCOL_ID, 15);
    memset(reserved, 0, 4);
    speed_mbps = hton32(speed);
    memcpy(info_hash, ih.data(), 20);
}

bool Handshake::validate_protocol_id() const {
    return memcmp(protocol_id, PROTOCOL_ID, 15) == 0
        && protocol_id[15] == 0 && protocol_id[16] == 0
        && protocol_id[17] == 0 && protocol_id[18] == 0;
}

std::vector<uint8_t> serialize_handshake(const Handshake& h) {
    std::vector<uint8_t> buf(67);
    memcpy(buf.data(), &h, 67);
    return buf;
}

bool parse_handshake(const uint8_t* data, size_t len, Handshake& h) {
    if (len < 67) return false;
    memcpy(&h, data, 67);
    if (!h.validate_protocol_id()) return false;
    h.speed_mbps = ntoh32(h.speed_mbps);
    return true;
}

std::vector<uint8_t> build_message(P2PMsgId id, const uint8_t* payload, uint32_t payload_len) {
    std::vector<uint8_t> buf(5 + payload_len);
    uint32_t len_be = hton32(1 + payload_len);
    memcpy(buf.data(), &len_be, 4);
    buf[4] = static_cast<uint8_t>(id);
    if (payload && payload_len > 0)
        memcpy(buf.data() + 5, payload, payload_len);
    return buf;
}

std::vector<uint8_t> build_have(uint32_t chunk_idx) {
    uint32_t idx_be = hton32(chunk_idx);
    return build_message(P2PMsgId::HAVE, reinterpret_cast<const uint8_t*>(&idx_be), 4);
}

std::vector<uint8_t> build_bitfield(const std::vector<bool>& have) {
    uint32_t byte_count = (static_cast<uint32_t>(have.size()) + 7) / 8;
    std::vector<uint8_t> bf(byte_count, 0);
    for (size_t i = 0; i < have.size(); i++)
        if (have[i]) bf[i / 8] |= (1u << (7 - (i % 8)));
    return build_message(P2PMsgId::BITFIELD, bf.data(), byte_count);
}

std::vector<uint8_t> build_request(uint32_t index, uint32_t begin, uint32_t length) {
    uint8_t payload[12];
    uint32_t idx_be = hton32(index), beg_be = hton32(begin), len_be = hton32(length);
    memcpy(payload,      &idx_be, 4);
    memcpy(payload + 4,  &beg_be, 4);
    memcpy(payload + 8,  &len_be, 4);
    return build_message(P2PMsgId::REQUEST, payload, 12);
}

std::vector<uint8_t> build_piece(uint32_t index, uint32_t begin, const uint8_t* data, uint32_t len) {
    // 一次性分配最终 buffer，消灭临时 payload 的冗余拷贝
    // 格式: [len_be:4][id:1][idx_be:4][beg_be:4][data:len]
    uint32_t msg_len = 1 + 8 + len; // id + header + data
    std::vector<uint8_t> buf(4 + msg_len);

    uint32_t len_be = hton32(msg_len);
    memcpy(buf.data(), &len_be, 4);
    buf[4] = static_cast<uint8_t>(P2PMsgId::PIECE);

    uint32_t idx_be = hton32(index), beg_be = hton32(begin);
    memcpy(buf.data() + 5,  &idx_be, 4);
    memcpy(buf.data() + 9,  &beg_be, 4);
    memcpy(buf.data() + 13, data, len);
    return buf;
}

std::vector<uint8_t> build_cancel(uint32_t index, uint32_t begin, uint32_t length) {
    uint8_t payload[12];
    uint32_t idx_be = hton32(index), beg_be = hton32(begin), len_be = hton32(length);
    memcpy(payload,      &idx_be, 4);
    memcpy(payload + 4,  &beg_be, 4);
    memcpy(payload + 8,  &len_be, 4);
    return build_message(P2PMsgId::CANCEL, payload, 12);
}

std::vector<uint8_t> build_interested() {
    return build_message(P2PMsgId::INTERESTED, nullptr, 0);
}

std::vector<uint8_t> build_not_interested() {
    return build_message(P2PMsgId::NOT_INTERESTED, nullptr, 0);
}

std::vector<uint8_t> build_pex(bool is_delta, const std::vector<PexPeer>& peers) {
    uint32_t plen = 3 + static_cast<uint32_t>(peers.size()) * 8;
    std::vector<uint8_t> payload(plen);
    payload[0] = is_delta ? 0x01 : 0x00;
    uint16_t count_be = hton16(static_cast<uint16_t>(peers.size()));
    memcpy(payload.data() + 1, &count_be, 2);
    for (size_t i = 0; i < peers.size(); i++) {
        PexPeer p = peers[i];
        p.ip   = hton32(p.ip);
        p.port = hton16(p.port);
        memcpy(payload.data() + 3 + i * 8, &p, 8);
    }
    return build_message(P2PMsgId::PEX, payload.data(), plen);
}

bool parse_message_header(const uint8_t* data, size_t len, uint32_t& msg_len, P2PMsgId& id) {
    if (len < 5) return false;
    uint32_t len_be;
    memcpy(&len_be, data, 4);
    msg_len = ntoh32(len_be);
    id = static_cast<P2PMsgId>(data[4]);
    return msg_len >= 1;
}

} // namespace thinbt
