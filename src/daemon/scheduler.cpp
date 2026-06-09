#include "scheduler.hpp"
#include <algorithm>
#include <climits>

namespace thinbt {

void Scheduler::init(uint32_t total_chunks, uint32_t local_speed_mbps,
                      RequestIssuer issue_req, HaveBroadcaster broadcast_have) {
    chunk_states_.resize(total_chunks, ChunkState::MISSING);
    availability_.resize(total_chunks, 0);
    chunk_requested_end_.resize(total_chunks, 0);
    missing_count_ = total_chunks;
    local_speed_mbps_ = local_speed_mbps;
    issue_request_  = std::move(issue_req);
    broadcast_have_ = std::move(broadcast_have);
}

void Scheduler::set_chunk_sizes(const std::vector<uint32_t>& sizes) {
    chunk_sub_blocks_.resize(sizes.size());
    for (size_t i = 0; i < sizes.size(); i++)
        chunk_sub_blocks_[i] = (sizes[i] + SUB_BLOCK_SIZE - 1) / SUB_BLOCK_SIZE;
}

void Scheduler::on_bitfield(uint32_t slot_id, const std::vector<bool>& bf) {
    auto* p = peer_slot(slot_id);
    if (!p) return;
    p->remote_bitfield = bf;
    for (size_t i = 0; i < bf.size() && i < availability_.size(); i++)
        if (bf[i]) availability_[i]++;
}

void Scheduler::on_have(uint32_t slot_id, uint32_t chunk_idx) {
    auto* p = peer_slot(slot_id);
    if (!p || chunk_idx >= p->remote_bitfield.size()) return;
    if (!p->remote_bitfield[chunk_idx]) {
        p->remote_bitfield[chunk_idx] = true;
        if (chunk_idx < availability_.size()) availability_[chunk_idx]++;
    }
}

void Scheduler::on_peer_added(uint32_t slot_id, uint32_t reported_speed) {
    PeerSlot ps;
    ps.slot_id = slot_id;
    ps.link_speed_reported = reported_speed;
    ps.link_speed_mbps = reported_speed;
    ps.remote_bitfield.resize(availability_.size(), false);
    peer_slots_.push_back(ps);
}

void Scheduler::on_peer_removed(uint32_t slot_id) {
    for (auto& p : peer_slots_) {
        if (p.slot_id == slot_id) {
            for (size_t i = 0; i < p.remote_bitfield.size(); i++)
                if (p.remote_bitfield[i] && i < availability_.size())
                    availability_[i]--;
            break;
        }
    }
    peer_slots_.erase(std::remove_if(peer_slots_.begin(), peer_slots_.end(),
        [slot_id](const auto& p) { return p.slot_id == slot_id; }), peer_slots_.end());
}

void Scheduler::on_choke_change(uint32_t slot_id, bool choking) {
    auto* p = peer_slot(slot_id);
    if (p) p->am_choking = choking;
}

void Scheduler::dec_peer_pending(uint32_t slot_id) {
    auto* p = peer_slot(slot_id);
    if (p && p->pending_requests > 0) p->pending_requests--;
}

PeerSlot* Scheduler::peer_slot(uint32_t id) {
    for (auto& p : peer_slots_) if (p.slot_id == id) return &p;
    return nullptr;
}

uint32_t Scheduler::select_best_peer(uint32_t chunk_idx) {
    uint32_t best_slot = UINT32_MAX;
    int best_score = INT_MIN;
    for (auto& p : peer_slots_) {
        if (chunk_idx >= p.remote_bitfield.size()) continue;
        if (!p.remote_bitfield[chunk_idx]) continue;
        if (p.am_choking) continue;
        if (p.consecutive_timeouts >= 3) continue;

        int score = 0;
        if (p.link_speed_mbps >= 1000 && local_speed_mbps_ >= 1000) score += 50;
        else if (local_speed_mbps_ >= 1000) score += 5;
        score -= static_cast<int>(p.pending_requests) * 3;
        score -= static_cast<int>(p.rtt_us) / 2000;
        if (score > best_score) { best_score = score; best_slot = p.slot_id; }
    }
    return best_slot;
}

void Scheduler::tick() {
    if (phase_ == SchedulerPhase::DONE) return;

    // 收集所有需要 sub-block 的 chunk（MISSING 或还有剩余 sub-block 的 REQUESTED）
    std::vector<std::pair<uint32_t, uint32_t>> pending;
    for (uint32_t i = 0; i < chunk_states_.size(); i++) {
        uint32_t total = i < chunk_sub_blocks_.size() ? chunk_sub_blocks_[i] : 8;
        uint32_t done  = i < chunk_requested_end_.size() ? chunk_requested_end_[i] : 0;
        if (chunk_states_[i] == ChunkState::COMPLETE) continue;
        if (done >= total) {
            if (chunk_states_[i] == ChunkState::MISSING) continue; // 空 chunk
            continue;
        }
        // MISSING 或 REQUESTED 但还没发完 sub-block
        pending.emplace_back(availability_[i], i);
    }
    if (pending.empty()) return;

    std::sort(pending.begin(), pending.end());  // rarest first

    for (auto& [avail, ci] : pending) {
        uint32_t total = ci < chunk_sub_blocks_.size() ? chunk_sub_blocks_[ci] : 8;
        uint32_t done  = ci < chunk_requested_end_.size() ? chunk_requested_end_[ci] : 0;
        if (done >= total) continue;

        uint32_t peer = select_best_peer(ci);
        if (peer == UINT32_MAX) continue;

        auto* p = peer_slot(peer);
        if (!p) continue;
        uint32_t cap = p->pipeline_cap;

        // 尽可能多发 sub-block，不超过 pipeline 余量和 chunk 剩余量
        uint32_t remaining = total - done;
        uint32_t to_issue = std::min(cap - p->pending_requests, remaining);
        if (to_issue == 0) continue;

        chunk_states_[ci] = ChunkState::REQUESTED;
        for (uint32_t b = 0; b < to_issue; b++) {
            uint32_t begin = (done + b) * SUB_BLOCK_SIZE;
            uint32_t len   = SUB_BLOCK_SIZE;
            issue_request_(peer, ci, begin, len);
            p->pending_requests++;
        }
        chunk_requested_end_[ci] = done + to_issue;
    }
}

void Scheduler::mark_all_complete(const std::vector<bool>& bitfield) {
    for (size_t i = 0; i < bitfield.size() && i < chunk_states_.size(); i++) {
        if (bitfield[i]) {
            if (chunk_states_[i] == ChunkState::MISSING) missing_count_--;
            chunk_states_[i] = ChunkState::COMPLETE;
        }
    }
    if (missing_count_ == 0) phase_ = SchedulerPhase::DONE;
}

void Scheduler::process_completions(std::vector<ChunkCompleteMsg>& completions) {
    for (auto& msg : completions) {
        chunk_states_[msg.chunk_idx] = ChunkState::COMPLETE;
        missing_count_--;
        broadcast_have_(msg.chunk_idx);
    }
    completions.clear();
    if (missing_count_ == 0) phase_ = SchedulerPhase::DONE;
    else if (missing_count_ < 128) phase_ = SchedulerPhase::ENDGAME;
}

} // namespace thinbt
