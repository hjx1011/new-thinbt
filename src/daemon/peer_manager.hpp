#ifndef THINBT_PEER_MANAGER_HPP
#define THINBT_PEER_MANAGER_HPP

#include "peer_session.hpp"
#include "protocol.hpp"
#include "scheduler.hpp"
#include <memory>
#include <vector>
#include <string>
#include <cstdint>

namespace thinbt {

class PeerManager {
public:
    PeerManager(Scheduler& sched, const Sha1Digest& info_hash, uint32_t local_speed_mbps);

    void add_peer(std::shared_ptr<PeerSession> sess);
    void remove_peer(uint32_t slot_id);
    void tick_choke();
    void tick_pex();

    size_t peer_count() const { return sessions_.size(); }
    uint32_t next_slot_id() { return next_slot_id_++; }

private:
    Scheduler& sched_;
    Sha1Digest info_hash_;
    uint32_t local_speed_mbps_;
    uint32_t next_slot_id_ = 0;
    std::vector<std::shared_ptr<PeerSession>> sessions_;
};

} // namespace thinbt
#endif
