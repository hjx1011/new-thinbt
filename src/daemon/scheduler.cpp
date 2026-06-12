#include "scheduler.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <random>
#include <utility>

namespace thinbt {

static uint64_t monotonic_ms() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
}

static size_t peer_have_count(const PeerSlot& peer) {
    return std::count(peer.remote_bitfield.begin(), peer.remote_bitfield.end(), true);
}

static bool is_full_source(const PeerSlot& peer, size_t total_chunks) {
    return total_chunks > 0 && peer.remote_bitfield.size() >= total_chunks
        && peer_have_count(peer) >= total_chunks;
}

static bool has_partial_sources(const std::vector<PeerSlot>& peers, size_t total_chunks) {
    for (const auto& peer : peers) {
        size_t have = peer_have_count(peer);
        if (have > 0 && have < total_chunks)
            return true;
    }
    return false;
}

void Scheduler::init(uint32_t total_chunks, uint32_t local_speed_mbps,
                     RequestIssuer issue_req, HaveBroadcaster broadcast_have,
                     NotInterestedBroadcaster broadcast_not_interested) {
    chunk_states_.assign(total_chunks, ChunkState::MISSING);
    availability_.assign(total_chunks, 0);
    chunk_sub_done_.resize(total_chunks);
    chunk_sub_req_.resize(total_chunks);
    chunk_sub_req_time_.resize(total_chunks);
    chunk_sub_done_count_.assign(total_chunks, 0);
    chunk_sub_req_count_.assign(total_chunks, 0);
    active_chunks_.clear();
    active_chunks_.reserve(total_chunks);
    for (uint32_t i = 0; i < total_chunks; ++i)
        active_chunks_.push_back(i);

    missing_count_ = total_chunks;
    phase_ = SchedulerPhase::NORMAL;
    local_speed_mbps_ = local_speed_mbps;
    issue_request_ = std::move(issue_req);
    broadcast_have_ = std::move(broadcast_have);
    broadcast_not_interested_ = std::move(broadcast_not_interested);
}

void Scheduler::set_cancel_issuer(CancelIssuer cancel) {
    cancel_request_ = std::move(cancel);
}

void Scheduler::randomize_piece_order(uint32_t seed) {
    std::mt19937 rng(seed);
    std::shuffle(active_chunks_.begin(), active_chunks_.end(), rng);
}

void Scheduler::set_not_interested_issuer(NotInterestedIssuer issuer) {
    not_interested_issuer_ = std::move(issuer);
}

void Scheduler::set_chunk_sizes(const std::vector<uint32_t>& sizes) {
    chunk_sub_blocks_.resize(sizes.size());
    for (size_t i = 0; i < sizes.size(); ++i) {
        uint32_t count = (sizes[i] + SUB_BLOCK_SIZE - 1) / SUB_BLOCK_SIZE;
        chunk_sub_blocks_[i] = count;
        if (i < chunk_sub_done_.size()) {
            chunk_sub_done_[i].assign(count, false);
            chunk_sub_req_[i].assign(count, false);
            chunk_sub_req_time_[i].assign(count, 0);
        }
    }
}

void Scheduler::on_bitfield(uint32_t slot_id, const std::vector<bool>& bf) {
    auto* peer = peer_slot(slot_id);
    if (!peer) return;
    peer->remote_bitfield = bf;
    for (size_t i = 0; i < bf.size() && i < availability_.size(); ++i) {
        if (bf[i]) availability_[i]++;
    }
    check_peer_interest(slot_id);
    rebalance_seed_requests_for_peer(*peer, monotonic_ms(), 16);
}

void Scheduler::on_have(uint32_t slot_id, uint32_t chunk_idx) {
    auto* peer = peer_slot(slot_id);
    if (!peer || chunk_idx >= peer->remote_bitfield.size()) return;
    if (!peer->remote_bitfield[chunk_idx]) {
        peer->remote_bitfield[chunk_idx] = true;
        if (chunk_idx < availability_.size()) availability_[chunk_idx]++;
    }
    check_peer_interest(slot_id);
    rebalance_seed_requests_for_peer(*peer, monotonic_ms(), 8);
}

bool Scheduler::wants_peer(uint32_t slot_id) const {
    const PeerSlot* peer = nullptr;
    for (const auto& candidate : peer_slots_) {
        if (candidate.slot_id == slot_id) {
            peer = &candidate;
            break;
        }
    }
    if (!peer) return false;

    for (size_t i = 0; i < peer->remote_bitfield.size() && i < chunk_states_.size(); ++i) {
        if (peer->remote_bitfield[i] && chunk_states_[i] != ChunkState::COMPLETE)
            return true;
    }
    return false;
}

void Scheduler::on_peer_added(uint32_t slot_id, uint32_t reported_speed) {
    PeerSlot slot;
    slot.slot_id = slot_id;
    slot.link_speed_reported = reported_speed;
    slot.link_speed_mbps = reported_speed;
    slot.remote_bitfield.assign(availability_.size(), false);
    peer_slots_.push_back(std::move(slot));
}

void Scheduler::on_peer_speed(uint32_t slot_id, uint32_t reported_speed) {
    auto* peer = peer_slot(slot_id);
    if (!peer) return;
    peer->link_speed_reported = reported_speed;
    peer->link_speed_mbps = reported_speed;
}

void Scheduler::on_peer_removed(uint32_t slot_id) {
    for (auto& peer : peer_slots_) {
        if (peer.slot_id != slot_id) continue;

        for (size_t i = 0; i < peer.remote_bitfield.size(); ++i) {
            if (peer.remote_bitfield[i] && i < availability_.size())
                availability_[i]--;
        }

        // Requests owned by a dead peer will never timeout or complete. Release them
        // now so normal scheduling can pick them up immediately.
        for (const auto& [chunk_idx, begins] : peer.active_sub_blocks) {
            for (uint32_t begin : begins) {
                uint32_t sub_idx = begin / SUB_BLOCK_SIZE;
                if (chunk_idx < chunk_sub_req_.size() &&
                    sub_idx < chunk_sub_req_[chunk_idx].size() &&
                    chunk_sub_req_[chunk_idx][sub_idx]) {
                    chunk_sub_req_[chunk_idx][sub_idx] = false;
                    if (chunk_idx < chunk_sub_req_time_.size() &&
                        sub_idx < chunk_sub_req_time_[chunk_idx].size()) {
                        chunk_sub_req_time_[chunk_idx][sub_idx] = 0;
                    }
                    if (chunk_idx < chunk_sub_req_count_.size() &&
                        chunk_sub_req_count_[chunk_idx] > 0) {
                        chunk_sub_req_count_[chunk_idx]--;
                    }
                }
            }

            if (chunk_idx < chunk_states_.size() &&
                chunk_states_[chunk_idx] != ChunkState::COMPLETE &&
                chunk_idx < chunk_sub_req_count_.size() &&
                chunk_sub_req_count_[chunk_idx] == 0) {
                chunk_states_[chunk_idx] = ChunkState::MISSING;
            }
        }
        break;
    }

    peer_slots_.erase(std::remove_if(peer_slots_.begin(), peer_slots_.end(),
        [slot_id](const auto& peer) { return peer.slot_id == slot_id; }), peer_slots_.end());
}

void Scheduler::on_choke_change(uint32_t slot_id, bool choking) {
    auto* peer = peer_slot(slot_id);
    if (peer) peer->am_choking = choking;
}

void Scheduler::dec_peer_pending(uint32_t slot_id, uint32_t chunk_idx, uint32_t begin) {
    auto* peer = peer_slot(slot_id);
    if (!peer || peer->pending_requests == 0) return;
    peer->pending_requests--;

    uint32_t sub_idx = begin / SUB_BLOCK_SIZE;
    if (chunk_idx < chunk_sub_done_.size() && sub_idx < chunk_sub_done_[chunk_idx].size()) {
        if (!chunk_sub_done_[chunk_idx][sub_idx]) {
            chunk_sub_done_[chunk_idx][sub_idx] = true;
            chunk_sub_done_count_[chunk_idx]++;
        }
    }

    auto it = peer->active_sub_blocks.find(chunk_idx);
    if (it != peer->active_sub_blocks.end()) {
        it->second.erase(begin);
        if (it->second.empty())
            peer->active_sub_blocks.erase(it);
    }

    if (phase_ != SchedulerPhase::DONE) {
        try_schedule_for_peer(*peer, monotonic_ms());
    }
}

PeerSlot* Scheduler::peer_slot(uint32_t id) {
    for (auto& peer : peer_slots_) {
        if (peer.slot_id == id) return &peer;
    }
    return nullptr;
}

std::optional<uint32_t> Scheduler::select_best_peer(uint32_t chunk_idx) {
    uint32_t best_slot = 0;
    int best_score = INT_MIN;
    size_t best_peer_have_count = SIZE_MAX;

    for (auto& peer : peer_slots_) {
        if (chunk_idx >= peer.remote_bitfield.size()) continue;
        if (!peer.remote_bitfield[chunk_idx]) continue;
        if (peer.am_choking) continue;
        if (peer.consecutive_timeouts >= 3) continue;

        size_t have_count = peer_have_count(peer);
        int score = 0;
        if (local_speed_mbps_ >= 1000 && peer.link_speed_mbps >= 1000) score += 10;
        score += static_cast<int>(
            std::min<size_t>(50, peer.remote_bitfield.size() > have_count
                ? (peer.remote_bitfield.size() - have_count) / 64
                : 0));
        score -= static_cast<int>(peer.pending_requests) * 6;
        score -= static_cast<int>(peer.rtt_us) / 2000;
        if (score > best_score ||
            (score == best_score && have_count < best_peer_have_count)) {
            best_score = score;
            best_slot = peer.slot_id;
            best_peer_have_count = have_count;
        }
    }

    if (best_score == INT_MIN) return std::nullopt;
    return best_slot;
}

static std::vector<uint32_t> find_redundant_peers(
    uint32_t chunk_idx, uint32_t exclude_slot, uint32_t count,
    const std::vector<PeerSlot>& slots) {
    std::vector<std::pair<int, uint32_t>> candidates;
    for (const auto& peer : slots) {
        if (peer.slot_id == exclude_slot) continue;
        if (chunk_idx >= peer.remote_bitfield.size()) continue;
        if (!peer.remote_bitfield[chunk_idx]) continue;
        if (peer.am_choking) continue;
        if (peer.consecutive_timeouts >= 3) continue;

        auto it = peer.active_sub_blocks.find(chunk_idx);
        if (it != peer.active_sub_blocks.end() && !it->second.empty()) continue;

        candidates.emplace_back(static_cast<int>(peer.pending_requests), peer.slot_id);
    }

    std::sort(candidates.begin(), candidates.end());
    std::vector<uint32_t> result;
    for (size_t i = 0; i < candidates.size() && result.size() < count; ++i)
        result.push_back(candidates[i].second);
    return result;
}

bool Scheduler::try_schedule_chunk_on_peer(PeerSlot& peer, uint32_t chunk_idx, uint64_t now_ms, uint32_t effective_cap) {
    if (chunk_idx >= chunk_states_.size()) return false;
    if (chunk_states_[chunk_idx] == ChunkState::COMPLETE) return false;
    if (chunk_idx >= peer.remote_bitfield.size() || !peer.remote_bitfield[chunk_idx]) return false;
    if (peer.am_choking || peer.consecutive_timeouts >= 3) return false;

    auto best_peer = select_best_peer(chunk_idx);
    if (!best_peer || *best_peer != peer.slot_id) return false;

    uint32_t total = chunk_idx < chunk_sub_blocks_.size() ? chunk_sub_blocks_[chunk_idx] : 0;
    if (total == 0) return false;

    bool issued = false;
    chunk_states_[chunk_idx] = ChunkState::DOWNLOADING;
    for (uint32_t sub_idx = 0; sub_idx < total; ++sub_idx) {
        if (peer.pending_requests >= effective_cap) break;
        if (chunk_sub_req_[chunk_idx][sub_idx]) continue;

        chunk_sub_req_[chunk_idx][sub_idx] = true;
        chunk_sub_req_time_[chunk_idx][sub_idx] = now_ms;
        if (chunk_idx < chunk_sub_req_count_.size()) chunk_sub_req_count_[chunk_idx]++;

        uint32_t begin = sub_idx * SUB_BLOCK_SIZE;
        issue_request_(peer.slot_id, chunk_idx, begin, SUB_BLOCK_SIZE);
        peer.pending_requests++;
        peer.active_sub_blocks[chunk_idx].insert(begin);
        issued = true;
    }

    return issued;
}

bool Scheduler::try_schedule_for_peer(PeerSlot& peer, uint64_t now_ms) {
    uint32_t effective_cap = peer.pipeline_cap;
    if (is_full_source(peer, chunk_states_.size()) &&
        has_partial_sources(peer_slots_, chunk_states_.size())) {
        effective_cap = std::min<uint32_t>(effective_cap, 32);
    }
    if (peer.pending_requests >= effective_cap) return false;

    std::vector<std::pair<uint32_t, uint32_t>> pending;
    pending.reserve(active_chunks_.size());
    for (uint32_t chunk_idx : active_chunks_) {
        if (chunk_idx >= chunk_states_.size()) continue;
        if (chunk_states_[chunk_idx] == ChunkState::COMPLETE) continue;
        if (chunk_idx >= peer.remote_bitfield.size() || !peer.remote_bitfield[chunk_idx]) continue;

        uint32_t total = chunk_idx < chunk_sub_blocks_.size() ? chunk_sub_blocks_[chunk_idx] : 0;
        uint32_t requested = chunk_idx < chunk_sub_req_count_.size() ? chunk_sub_req_count_[chunk_idx] : 0;
        if (requested >= total) continue;

        pending.emplace_back(availability_[chunk_idx], chunk_idx);
    }

    std::stable_sort(pending.begin(), pending.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });

    bool issued = false;
    for (const auto& [avail, chunk_idx] : pending) {
        (void)avail;
        if (peer.pending_requests >= effective_cap) break;
        if (try_schedule_chunk_on_peer(peer, chunk_idx, now_ms, effective_cap))
            issued = true;
    }

    return issued;
}

void Scheduler::rebalance_seed_requests_for_peer(PeerSlot& peer, uint64_t now_ms, uint32_t max_moves) {
    if (is_full_source(peer, chunk_states_.size())) return;
    uint32_t effective_cap = peer.pipeline_cap;
    uint32_t moves = 0;
    while (peer.pending_requests < effective_cap && moves < max_moves) {
        bool moved = false;
        for (uint32_t chunk_idx : active_chunks_) {
            if (peer.pending_requests >= effective_cap) break;
            if (chunk_idx >= peer.remote_bitfield.size() || !peer.remote_bitfield[chunk_idx]) continue;
            if (chunk_idx >= chunk_states_.size() || chunk_states_[chunk_idx] == ChunkState::COMPLETE) continue;

            for (auto& source : peer_slots_) {
                if (source.slot_id == peer.slot_id) continue;
                if (!is_full_source(source, chunk_states_.size())) continue;
                auto it = source.active_sub_blocks.find(chunk_idx);
                if (it == source.active_sub_blocks.end() || it->second.empty()) continue;

                uint32_t begin = *it->second.begin();
                uint32_t sub_idx = begin / SUB_BLOCK_SIZE;
                if (chunk_idx < chunk_sub_done_.size() &&
                    sub_idx < chunk_sub_done_[chunk_idx].size() &&
                    chunk_sub_done_[chunk_idx][sub_idx]) {
                    it->second.erase(begin);
                    continue;
                }

                if (cancel_request_) cancel_request_(source.slot_id, chunk_idx, begin, SUB_BLOCK_SIZE);
                if (source.pending_requests > 0) source.pending_requests--;
                it->second.erase(begin);
                if (it->second.empty()) source.active_sub_blocks.erase(it);

                chunk_sub_req_[chunk_idx][sub_idx] = true;
                chunk_sub_req_time_[chunk_idx][sub_idx] = now_ms;
                issue_request_(peer.slot_id, chunk_idx, begin, SUB_BLOCK_SIZE);
                peer.pending_requests++;
                peer.active_sub_blocks[chunk_idx].insert(begin);
                moves++;
                moved = true;
                break;
            }
            if (moved) break;
        }
        if (!moved) break;
    }
}

void Scheduler::tick() {
    if (phase_ == SchedulerPhase::DONE) return;

    uint64_t now = monotonic_ms();
    for (auto& peer : peer_slots_) {
        rebalance_seed_requests_for_peer(peer, now, 4);
    }
    for (auto& peer : peer_slots_) {
        try_schedule_for_peer(peer, now);
    }

    if (phase_ == SchedulerPhase::ENDGAME)
        tick_endgame(now);
}

void Scheduler::tick_endgame(uint64_t now_ms) {
    std::vector<uint32_t> downloading;
    for (uint32_t chunk_idx : active_chunks_) {
        if (chunk_idx < chunk_states_.size() &&
            chunk_states_[chunk_idx] == ChunkState::DOWNLOADING) {
            downloading.push_back(chunk_idx);
        }
    }
    if (downloading.empty()) return;

    std::sort(downloading.begin(), downloading.end(), [this](uint32_t a, uint32_t b) {
        uint32_t done_a = a < chunk_sub_done_count_.size() ? chunk_sub_done_count_[a] : 0;
        uint32_t done_b = b < chunk_sub_done_count_.size() ? chunk_sub_done_count_[b] : 0;
        uint32_t total_a = a < chunk_sub_blocks_.size() ? chunk_sub_blocks_[a] : 0;
        uint32_t total_b = b < chunk_sub_blocks_.size() ? chunk_sub_blocks_[b] : 0;
        uint32_t rem_a = total_a > done_a ? total_a - done_a : 0;
        uint32_t rem_b = total_b > done_b ? total_b - done_b : 0;
        return rem_a < rem_b;
    });

    uint32_t endgame_processed = 0;
    for (uint32_t chunk_idx : downloading) {
        if (endgame_processed >= MAX_ENDGAME_CHUNKS) break;

        uint32_t total = chunk_idx < chunk_sub_blocks_.size() ? chunk_sub_blocks_[chunk_idx] : 0;
        if (total == 0 || chunk_idx >= chunk_sub_req_time_.size()) continue;

        uint32_t primary_peer = UINT32_MAX;
        size_t max_active = 0;
        for (const auto& peer : peer_slots_) {
            auto it = peer.active_sub_blocks.find(chunk_idx);
            if (it != peer.active_sub_blocks.end() && it->second.size() > max_active) {
                max_active = it->second.size();
                primary_peer = peer.slot_id;
            }
        }

        uint64_t hunger_threshold_ms = 50;
        if (primary_peer != UINT32_MAX) {
            auto* peer = peer_slot(primary_peer);
            if (peer && peer->rtt_us > 0)
                hunger_threshold_ms = std::max<uint64_t>(peer->rtt_us * 2 / 1000, 50);
        }

        bool any_hungry = false;
        for (uint32_t sub_idx = 0; sub_idx < total; ++sub_idx) {
            if (chunk_idx < chunk_sub_done_.size() &&
                sub_idx < chunk_sub_done_[chunk_idx].size() &&
                chunk_sub_done_[chunk_idx][sub_idx]) {
                continue;
            }
            if (chunk_idx >= chunk_sub_req_.size() ||
                sub_idx >= chunk_sub_req_[chunk_idx].size() ||
                !chunk_sub_req_[chunk_idx][sub_idx]) {
                continue;
            }

            uint64_t req_time =
                (chunk_idx < chunk_sub_req_time_.size() && sub_idx < chunk_sub_req_time_[chunk_idx].size())
                    ? chunk_sub_req_time_[chunk_idx][sub_idx]
                    : 0;
            if (req_time == 0) continue;
            if ((now_ms - req_time) <= hunger_threshold_ms) continue;

            any_hungry = true;
            auto redundant = find_redundant_peers(
                chunk_idx, primary_peer, MAX_REDUNDANT_PEERS, peer_slots_);
            for (uint32_t peer_slot_id : redundant) {
                auto* peer = peer_slot(peer_slot_id);
                if (!peer) continue;
                uint32_t begin = sub_idx * SUB_BLOCK_SIZE;
                issue_request_(peer_slot_id, chunk_idx, begin, SUB_BLOCK_SIZE);
                peer->pending_requests++;
                peer->active_sub_blocks[chunk_idx].insert(begin);
            }
        }

        if (any_hungry) endgame_processed++;
    }
}

void Scheduler::send_cancel_for_chunk(uint32_t chunk_idx, uint32_t exclude_slot) {
    if (!cancel_request_) return;

    for (auto& peer : peer_slots_) {
        if (peer.slot_id == exclude_slot) continue;
        auto it = peer.active_sub_blocks.find(chunk_idx);
        if (it == peer.active_sub_blocks.end() || it->second.empty()) continue;

        for (uint32_t begin : it->second) {
            cancel_request_(peer.slot_id, chunk_idx, begin, SUB_BLOCK_SIZE);
            if (peer.pending_requests > 0)
                peer.pending_requests--;
        }
        it->second.clear();
        peer.active_sub_blocks.erase(it);
    }
}

void Scheduler::process_completions(std::vector<ChunkCompleteMsg>& completions) {
    for (const auto& msg : completions) {
        chunk_states_[msg.chunk_idx] = ChunkState::COMPLETE;
        missing_count_--;

        auto it = std::find(active_chunks_.begin(), active_chunks_.end(), msg.chunk_idx);
        if (it != active_chunks_.end()) {
            *it = active_chunks_.back();
            active_chunks_.pop_back();
        }

        send_cancel_for_chunk(msg.chunk_idx, msg.winning_peer_slot);
        if (msg.chunk_idx < chunk_sub_req_count_.size())
            chunk_sub_req_count_[msg.chunk_idx] = 0;
        broadcast_have_(msg.chunk_idx);
    }

    completions.clear();
    if (missing_count_ == 0) {
        phase_ = SchedulerPhase::DONE;
        if (broadcast_not_interested_) broadcast_not_interested_();
    } else if (missing_count_ < ENDGAME_THRESHOLD) {
        phase_ = SchedulerPhase::ENDGAME;
    }
}

void Scheduler::on_verify_failed(uint32_t chunk_idx) {
    chunk_states_[chunk_idx] = ChunkState::MISSING;
    if (chunk_idx < chunk_sub_req_.size())
        std::fill(chunk_sub_req_[chunk_idx].begin(), chunk_sub_req_[chunk_idx].end(), false);
    if (chunk_idx < chunk_sub_req_time_.size())
        std::fill(chunk_sub_req_time_[chunk_idx].begin(), chunk_sub_req_time_[chunk_idx].end(), 0);
    if (chunk_idx < chunk_sub_done_.size())
        std::fill(chunk_sub_done_[chunk_idx].begin(), chunk_sub_done_[chunk_idx].end(), false);
    if (chunk_idx < chunk_sub_done_count_.size())
        chunk_sub_done_count_[chunk_idx] = 0;
    if (chunk_idx < chunk_sub_req_count_.size())
        chunk_sub_req_count_[chunk_idx] = 0;

    if (std::find(active_chunks_.begin(), active_chunks_.end(), chunk_idx) == active_chunks_.end())
        active_chunks_.push_back(chunk_idx);
}

void Scheduler::on_subblock_timeout(uint32_t chunk_idx, uint32_t begin) {
    if (chunk_idx >= chunk_states_.size()) return;
    if (chunk_states_[chunk_idx] == ChunkState::COMPLETE) return;

    uint32_t sub_idx = begin / SUB_BLOCK_SIZE;
    if (chunk_idx < chunk_sub_req_.size() && sub_idx < chunk_sub_req_[chunk_idx].size()) {
        if (chunk_sub_req_[chunk_idx][sub_idx]) {
            chunk_sub_req_[chunk_idx][sub_idx] = false;
            if (chunk_idx < chunk_sub_req_count_.size() &&
                chunk_sub_req_count_[chunk_idx] > 0) {
                chunk_sub_req_count_[chunk_idx]--;
            }
        }
    }
    if (chunk_idx < chunk_sub_req_time_.size() &&
        sub_idx < chunk_sub_req_time_[chunk_idx].size()) {
        chunk_sub_req_time_[chunk_idx][sub_idx] = 0;
    }
}

void Scheduler::check_peer_interest(uint32_t slot_id) {
    if (!not_interested_issuer_) return;
    if (!wants_peer(slot_id))
        not_interested_issuer_(slot_id);
}

void Scheduler::mark_all_complete(const std::vector<bool>& bitfield) {
    for (size_t i = 0; i < bitfield.size() && i < chunk_states_.size(); ++i) {
        if (!bitfield[i]) continue;
        if (chunk_states_[i] == ChunkState::COMPLETE) continue;

        if (chunk_states_[i] == ChunkState::MISSING)
            missing_count_--;
        chunk_states_[i] = ChunkState::COMPLETE;

        auto it = std::find(active_chunks_.begin(), active_chunks_.end(), static_cast<uint32_t>(i));
        if (it != active_chunks_.end()) {
            *it = active_chunks_.back();
            active_chunks_.pop_back();
        }
        if (i < chunk_sub_req_count_.size())
            chunk_sub_req_count_[i] = 0;
    }

    if (missing_count_ == 0) phase_ = SchedulerPhase::DONE;
    else if (missing_count_ < ENDGAME_THRESHOLD) phase_ = SchedulerPhase::ENDGAME;
}

} // namespace thinbt
