#ifndef THINBT_SCHEDULER_HPP
#define THINBT_SCHEDULER_HPP

#include "common/platform.hpp"
#include "chunk_assembler.hpp"
#include "protocol.hpp"
#include <vector>
#include <functional>

namespace thinbt {

struct PeerSlot {
    uint32_t slot_id = 0;
    uint32_t pending_requests = 0;
    uint32_t consecutive_timeouts = 0;
    uint32_t rtt_us = 0;
    uint32_t link_speed_mbps = 0;
    uint32_t link_speed_reported = 0;
    uint32_t pipeline_cap = 16;
    bool     am_choking = true;
    std::vector<bool> remote_bitfield;
};

enum class SchedulerPhase { NORMAL, ENDGAME, DONE };
enum class ChunkState : uint8_t { MISSING, REQUESTED, DOWNLOADING, COMPLETE };

class Scheduler {
public:
    using RequestIssuer = std::function<void(uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length)>;
    using HaveBroadcaster = std::function<void(uint32_t chunk_idx)>;

    void init(uint32_t total_chunks, uint32_t local_speed_mbps,
              RequestIssuer issue_req, HaveBroadcaster broadcast_have);

    // Event-driven availability (O(1) per event)
    void on_bitfield(uint32_t slot_id, const std::vector<bool>& bf);
    void on_have(uint32_t slot_id, uint32_t chunk_idx);
    void on_peer_added(uint32_t slot_id, uint32_t reported_speed);
    void on_peer_removed(uint32_t slot_id);

    void tick();
    void process_completions(std::vector<ChunkCompleteMsg>& completions);

    SchedulerPhase phase() const { return phase_; }
    uint32_t missing_count() const { return missing_count_; }
    PeerSlot* peer_slot(uint32_t id);

private:
    uint32_t select_best_peer(uint32_t chunk_idx);

    std::vector<ChunkState> chunk_states_;
    std::vector<uint32_t>  availability_;
    std::vector<PeerSlot>  peer_slots_;
    SchedulerPhase phase_ = SchedulerPhase::NORMAL;
    uint32_t missing_count_ = 0;
    uint32_t local_speed_mbps_ = 1000;
    RequestIssuer issue_request_;
    HaveBroadcaster broadcast_have_;
};

} // namespace thinbt
#endif
