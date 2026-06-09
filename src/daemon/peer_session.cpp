#include "peer_session.hpp"
#include "common/net_util.hpp"
#include <cstring>
#include <cstdlib>

namespace thinbt {

PeerSession::PeerSession(void* /*socket_ptr*/, const Sha1Digest& info_hash, uint32_t local_speed_mbps)
    : local_speed_mbps_(local_speed_mbps) {
    memcpy(our_info_hash_.data(), info_hash.data(), 20);
}

PeerSession::~PeerSession() = default;

void PeerSession::start(OnMessage on_msg, OnDisconnect on_disc) {
    on_message_ = std::move(on_msg);
    on_disconnect_ = std::move(on_disc);
}

void PeerSession::send_message(const std::vector<uint8_t>& bytes) {
    (void)bytes;
}

void PeerSession::disconnect() {}

std::string PeerSession::remote_ip() const {
    return "0.0.0.0";
}

void PeerSession::record_have(uint32_t chunk_idx) {
    if (chunk_idx < remote_bitfield_.size())
        remote_bitfield_[chunk_idx] = true;
}

void PeerSession::record_bitfield(const uint8_t* data, uint32_t len) {
    remote_bitfield_.resize(len * 8);
    for (uint32_t i = 0; i < len * 8; i++)
        remote_bitfield_[i] = (data[i / 8] >> (7 - (i % 8))) & 1;
}

} // namespace thinbt
