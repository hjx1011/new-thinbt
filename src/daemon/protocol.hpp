#ifndef THINBT_PROTOCOL_HPP
#define THINBT_PROTOCOL_HPP

#include "common/platform.hpp"
#include "common/hash.hpp"
#include <vector>
#include <array>
#include <cstdint>

namespace thinbt {

enum class P2PMsgId : uint8_t {
    CHOKE          = 0,
    UNCHOKE        = 1,
    INTERESTED     = 2,
    NOT_INTERESTED = 3,
    HAVE           = 4,
    BITFIELD       = 5,
    REQUEST        = 6,
    PIECE          = 7,
    CANCEL         = 8,
    PEX            = 9,
};

// 替换 src/daemon/protocol.hpp 中对应的结构体定义

#pragma pack(push, 1)

struct Handshake {
    static constexpr const char* PROTOCOL_ID = "thinBT Protocol";
    static constexpr size_t PROTOCOL_ID_LEN  = 19;

    uint8_t  protocol_id[19] = {};
    uint8_t  reserved[4]     = {};
    uint32_t speed_mbps      = 0;
    uint8_t  info_hash[20]   = {};
    uint8_t  peer_id[20]     = {};

    void build(const Sha1Digest& ih, uint32_t speed);
    bool validate_protocol_id() const;
};
static_assert(sizeof(Handshake) == 67, "Handshake must be 67 bytes");

struct PexPeer {
    uint32_t ip;
    uint16_t port;
    uint8_t  flags;
    uint8_t  reserved;
};
static_assert(sizeof(PexPeer) == 8, "PexPeer must be 8 bytes");

#pragma pack(pop)

std::vector<uint8_t> serialize_handshake(const Handshake& h);
bool parse_handshake(const uint8_t* data, size_t len, Handshake& h);

std::vector<uint8_t> build_message(P2PMsgId id, const uint8_t* payload, uint32_t payload_len);
std::vector<uint8_t> build_have(uint32_t chunk_idx);
std::vector<uint8_t> build_bitfield(const std::vector<bool>& have);
std::vector<uint8_t> build_request(uint32_t index, uint32_t begin, uint32_t length);
std::vector<uint8_t> build_piece(uint32_t index, uint32_t begin, const uint8_t* data, uint32_t len);
std::vector<uint8_t> build_cancel(uint32_t index, uint32_t begin, uint32_t length);
std::vector<uint8_t> build_interested();
std::vector<uint8_t> build_not_interested();
std::vector<uint8_t> build_pex(bool is_delta, const std::vector<PexPeer>& peers);

bool parse_message_header(const uint8_t* data, size_t len, uint32_t& msg_len, P2PMsgId& id);

} // namespace thinbt
#endif
