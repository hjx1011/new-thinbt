#include "peer_manager.hpp"
#include <algorithm>
#include <random>
#include <cstring>

namespace thinbt {

PeerManager::PeerManager(Scheduler& sched, const Sha1Digest& info_hash, uint32_t local_speed_mbps)
    : sched_(sched), local_speed_mbps_(local_speed_mbps) {
    memcpy(info_hash_.data(), info_hash.data(), 20);
}

void PeerManager::add_peer(std::shared_ptr<PeerSession> sess) {
    uint32_t id = next_slot_id_++;
    sched_.on_peer_added(id, sess->link_speed_reported());
    sessions_.push_back(sess);
}

void PeerManager::remove_peer(uint32_t slot_id) {
    sched_.on_peer_removed(slot_id);
    sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(),
        [](const auto& s) { return !s; }), sessions_.end());
}

void PeerManager::tick_choke() {
    if (sessions_.empty()) return;
    // Simplified: unchoke all for LAN classroom
    for (auto& s : sessions_) s->set_choked(false);
}

void PeerManager::tick_pex() {
    // Stub: full PEX Delta updates in integration
}

} // namespace thinbt
