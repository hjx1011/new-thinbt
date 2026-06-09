#include "scheduler.hpp"
#include <algorithm>
#include <climits>
#include <chrono>

namespace thinbt {

static uint64_t monotonic_ms() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
}

void Scheduler::init(uint32_t total_chunks, uint32_t local_speed_mbps,
                      RequestIssuer issue_req, HaveBroadcaster broadcast_have,
                      NotInterestedBroadcaster broadcast_not_interested) {
    chunk_states_.resize(total_chunks, ChunkState::MISSING);
    availability_.resize(total_chunks, 0);
    chunk_sub_done_.resize(total_chunks);
    chunk_sub_req_.resize(total_chunks);
    chunk_sub_req_time_.resize(total_chunks);
    chunk_sub_done_count_.resize(total_chunks, 0);
    missing_count_ = total_chunks;
    local_speed_mbps_ = local_speed_mbps;
    issue_request_  = std::move(issue_req);
    broadcast_have_ = std::move(broadcast_have);
    broadcast_not_interested_ = std::move(broadcast_not_interested);

    // 活跃 chunk 索引缓存（避免每 tick O(N) 全扫）
    active_chunks_.reserve(total_chunks);
    for (uint32_t i = 0; i < total_chunks; i++)
        active_chunks_.push_back(i);
}

void Scheduler::set_cancel_issuer(CancelIssuer cancel) {
    cancel_request_ = std::move(cancel);
}

void Scheduler::set_chunk_sizes(const std::vector<uint32_t>& sizes) {
    chunk_sub_blocks_.resize(sizes.size());
    for (size_t i = 0; i < sizes.size(); i++) {
        uint32_t n = (sizes[i] + SUB_BLOCK_SIZE - 1) / SUB_BLOCK_SIZE;
        chunk_sub_blocks_[i] = n;
        if (i < chunk_sub_done_.size()) {
            chunk_sub_done_[i].resize(n, false);
            chunk_sub_req_[i].resize(n, false);
            chunk_sub_req_time_[i].resize(n, 0);
        }
    }
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
            // pending_requests 随 peer 移除归零，active_sub_blocks 自动析构
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

void Scheduler::dec_peer_pending(uint32_t slot_id, uint32_t chunk_idx, uint32_t begin) {
    auto* p = peer_slot(slot_id);
    if (!p || p->pending_requests == 0) return;
    p->pending_requests--;

    // 标记 sub-block 完成位图 + 累加完成计数
    uint32_t slot = begin / SUB_BLOCK_SIZE;
    if (chunk_idx < chunk_sub_done_.size() && slot < chunk_sub_done_[chunk_idx].size()) {
        if (!chunk_sub_done_[chunk_idx][slot]) {
            chunk_sub_done_[chunk_idx][slot] = true;
            chunk_sub_done_count_[chunk_idx]++;
        }
    }

    // 从 peer 的 active 集合中移除
    auto it = p->active_sub_blocks.find(chunk_idx);
    if (it != p->active_sub_blocks.end()) {
        it->second.erase(begin);
        if (it->second.empty())
            p->active_sub_blocks.erase(it);
    }
}

PeerSlot* Scheduler::peer_slot(uint32_t id) {
    for (auto& p : peer_slots_) if (p.slot_id == id) return &p;
    return nullptr;
}

std::optional<uint32_t> Scheduler::select_best_peer(uint32_t chunk_idx) {
    uint32_t best_slot = 0;
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
    if (best_score == INT_MIN) return std::nullopt;
    return best_slot;
}

// 为饥饿 sub-block 找最空闲的 count 个额外 Peer（排除 exclude_slot）
static std::vector<uint32_t> find_redundant_peers(
    uint32_t chunk_idx, uint32_t exclude_slot, uint32_t count,
    const std::vector<PeerSlot>& slots)
{
    std::vector<std::pair<int, uint32_t>> candidates; // (score, slot_id), score 越低越闲
    for (auto& p : slots) {
        if (p.slot_id == exclude_slot) continue;
        if (chunk_idx >= p.remote_bitfield.size()) continue;
        if (!p.remote_bitfield[chunk_idx]) continue;
        if (p.am_choking) continue;
        if (p.consecutive_timeouts >= 3) continue;

        // 检查该 peer 是否已经在传这个 chunk 的 sub-block
        auto it = p.active_sub_blocks.find(chunk_idx);
        if (it != p.active_sub_blocks.end() && !it->second.empty())
            continue; // 已在传，跳过（避免给同一 peer 发同 chunk 的重复请求）

        // pending_requests 越少越好
        candidates.emplace_back(static_cast<int>(p.pending_requests), p.slot_id);
    }
    std::sort(candidates.begin(), candidates.end());

    std::vector<uint32_t> result;
    for (size_t i = 0; i < candidates.size() && result.size() < count; i++)
        result.push_back(candidates[i].second);
    return result;
}

void Scheduler::tick() {
    if (phase_ == SchedulerPhase::DONE) return;

    uint64_t now = monotonic_ms();

    // ═══════════════════════════════════════════════════════
    // NORMAL 逻辑（所有阶段都执行）：Rarest First + 逐 sub-block 发请求
    // 使用 active_chunks_ 缓存，避免 O(N) 全扫 32768 个 chunk
    // ═══════════════════════════════════════════════════════
    std::vector<std::pair<uint32_t, uint32_t>> pending;
    for (uint32_t ci : active_chunks_) {
        if (ci >= chunk_states_.size()) continue;
        if (chunk_states_[ci] == ChunkState::COMPLETE) continue;
        uint32_t total = ci < chunk_sub_blocks_.size() ? chunk_sub_blocks_[ci] : 8;
        uint32_t req_count = ci < chunk_sub_req_.size()
            ? static_cast<uint32_t>(std::count(chunk_sub_req_[ci].begin(), chunk_sub_req_[ci].end(), true)) : 0;
        if (req_count >= total) {
            if (chunk_states_[ci] == ChunkState::MISSING) continue;
            continue;
        }
        pending.emplace_back(availability_[ci], ci);
    }
    if (!pending.empty()) {
        // 只处理前 BATCH_SIZE 个最稀缺的 chunk，避免对大列表全排序
        constexpr uint32_t BATCH_SIZE = 1024;
        if (pending.size() > BATCH_SIZE) {
            std::nth_element(pending.begin(), pending.begin() + BATCH_SIZE, pending.end());
            pending.resize(BATCH_SIZE);
        }
        std::sort(pending.begin(), pending.end());  // rarest first

        for (auto& [avail, ci] : pending) {
            uint32_t total = ci < chunk_sub_blocks_.size() ? chunk_sub_blocks_[ci] : 8;
            auto peer_opt = select_best_peer(ci);
            if (!peer_opt) continue;
            uint32_t peer = *peer_opt;

            auto* p = peer_slot(peer);
            if (!p) continue;
            uint32_t cap = p->pipeline_cap;

            chunk_states_[ci] = ChunkState::DOWNLOADING;

            for (uint32_t b = 0; b < total; b++) {
                if (p->pending_requests >= cap) break;
                if (!chunk_sub_req_[ci][b]) {
                    chunk_sub_req_[ci][b] = true;
                    chunk_sub_req_time_[ci][b] = now;
                    uint32_t begin = b * SUB_BLOCK_SIZE;
                    issue_request_(peer, ci, begin, SUB_BLOCK_SIZE);
                    p->pending_requests++;
                    p->active_sub_blocks[ci].insert(begin);
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════
    // ENDGAME 精准补漏逻辑
    // ═══════════════════════════════════════════════════════
    if (phase_ == SchedulerPhase::ENDGAME)
        tick_endgame(now);
}

void Scheduler::tick_endgame(uint64_t now_ms) {
    // 收集所有 DOWNLOADING 状态的 chunk（从活跃列表中筛选）
    std::vector<uint32_t> downloading;
    for (uint32_t ci : active_chunks_) {
        if (ci < chunk_states_.size() && chunk_states_[ci] == ChunkState::DOWNLOADING)
            downloading.push_back(ci);
    }
    if (downloading.empty()) return;

    // 优先处理剩余 sub-block 少的 chunk（接近完成的最需要加速）
    std::sort(downloading.begin(), downloading.end(), [this](uint32_t a, uint32_t b) {
        uint32_t done_a = a < chunk_sub_done_count_.size() ? chunk_sub_done_count_[a] : 0;
        uint32_t done_b = b < chunk_sub_done_count_.size() ? chunk_sub_done_count_[b] : 0;
        uint32_t total_a = a < chunk_sub_blocks_.size() ? chunk_sub_blocks_[a] : 0;
        uint32_t total_b = b < chunk_sub_blocks_.size() ? chunk_sub_blocks_[b] : 0;
        uint32_t rem_a = total_a > done_a ? total_a - done_a : 0;
        uint32_t rem_b = total_b > done_b ? total_b - done_b : 0;
        return rem_a < rem_b;  // 剩余少的优先
    });

    uint32_t endgame_processed = 0;
    for (uint32_t ci : downloading) {
        if (endgame_processed >= MAX_ENDGAME_CHUNKS) break;

        uint32_t total = ci < chunk_sub_blocks_.size() ? chunk_sub_blocks_[ci] : 0;
        if (total == 0 || ci >= chunk_sub_req_time_.size()) continue;

        // 找该 chunk 的主要负责 Peer（active_sub_blocks 最多的那个）
        uint32_t primary_peer = UINT32_MAX;
        size_t max_active = 0;
        for (auto& p : peer_slots_) {
            auto it = p.active_sub_blocks.find(ci);
            if (it != p.active_sub_blocks.end() && it->second.size() > max_active) {
                max_active = it->second.size();
                primary_peer = p.slot_id;
            }
        }

        // 饥饿阈值：2 倍主要 peer 的 RTT，至少 50ms
        uint64_t hunger_threshold_ms = 50;
        if (primary_peer != UINT32_MAX) {
            auto* pp = peer_slot(primary_peer);
            if (pp && pp->rtt_us > 0)
                hunger_threshold_ms = std::max<uint64_t>(pp->rtt_us * 2 / 1000, 50);
        }

        bool any_hungry = false;
        for (uint32_t s = 0; s < total; s++) {
            // 已完成或未请求的 sub-block 不参与饥饿判定
            if (ci < chunk_sub_done_.size() && s < chunk_sub_done_[ci].size() && chunk_sub_done_[ci][s])
                continue;
            if (ci >= chunk_sub_req_.size() || s >= chunk_sub_req_[ci].size() || !chunk_sub_req_[ci][s])
                continue;

            // 基于该 sub-block 自己的请求时间做饥饿判定，而非 chunk 级时间戳
            uint64_t sub_req_time = ci < chunk_sub_req_time_.size() && s < chunk_sub_req_time_[ci].size()
                ? chunk_sub_req_time_[ci][s] : 0;
            if (sub_req_time == 0) continue;
            if ((now_ms - sub_req_time) <= hunger_threshold_ms) continue;

            // 饥饿 sub-block：找 2 个最空闲的额外 Peer 发冗余请求
            any_hungry = true;
            auto redundant = find_redundant_peers(ci, primary_peer, MAX_REDUNDANT_PEERS, peer_slots_);
            for (uint32_t rp : redundant) {
                auto* rpeer = peer_slot(rp);
                if (!rpeer) continue;
                uint32_t begin = s * SUB_BLOCK_SIZE;
                issue_request_(rp, ci, begin, SUB_BLOCK_SIZE);
                rpeer->pending_requests++;
                rpeer->active_sub_blocks[ci].insert(begin);
            }
        }

        if (any_hungry) endgame_processed++;
    }
}

void Scheduler::send_cancel_for_chunk(uint32_t chunk_idx, uint32_t exclude_slot) {
    if (!cancel_request_) return;

    for (auto& p : peer_slots_) {
        if (p.slot_id == exclude_slot) continue;  // 跳过赢家 Peer，它已经完成了
        auto it = p.active_sub_blocks.find(chunk_idx);
        if (it == p.active_sub_blocks.end() || it->second.empty()) continue;

        for (uint32_t begin : it->second) {
            cancel_request_(p.slot_id, chunk_idx, begin, SUB_BLOCK_SIZE);
        }
        it->second.clear();
    }
}

void Scheduler::process_completions(std::vector<ChunkCompleteMsg>& completions) {
    for (auto& msg : completions) {
        chunk_states_[msg.chunk_idx] = ChunkState::COMPLETE;
        missing_count_--;

        // 从活跃列表中移除已完成的 chunk
        auto it = std::find(active_chunks_.begin(), active_chunks_.end(), msg.chunk_idx);
        if (it != active_chunks_.end()) {
            *it = active_chunks_.back();
            active_chunks_.pop_back();
        }

        // Cancel 清理：排除赢家 Peer，取消其他所有还在传该 chunk 的冗余请求
        send_cancel_for_chunk(msg.chunk_idx, msg.winning_peer_slot);

        broadcast_have_(msg.chunk_idx);
    }
    completions.clear();
    if (missing_count_ == 0) {
        phase_ = SchedulerPhase::DONE;
        // 所有 chunk 完成，通知所有 peer 本节点不再需要数据
        if (broadcast_not_interested_) broadcast_not_interested_();
    } else if (missing_count_ < ENDGAME_THRESHOLD) {
        phase_ = SchedulerPhase::ENDGAME;
    }
}

void Scheduler::on_verify_failed(uint32_t chunk_idx) {
    // SHA-256 校验失败：回退为 MISSING，重置所有 sub-block 状态
    chunk_states_[chunk_idx] = ChunkState::MISSING;
    if (chunk_idx < chunk_sub_req_.size())
        std::fill(chunk_sub_req_[chunk_idx].begin(), chunk_sub_req_[chunk_idx].end(), false);
    if (chunk_idx < chunk_sub_req_time_.size())
        std::fill(chunk_sub_req_time_[chunk_idx].begin(), chunk_sub_req_time_[chunk_idx].end(), 0);
    if (chunk_idx < chunk_sub_done_.size())
        std::fill(chunk_sub_done_[chunk_idx].begin(), chunk_sub_done_[chunk_idx].end(), false);
    if (chunk_idx < chunk_sub_done_count_.size())
        chunk_sub_done_count_[chunk_idx] = 0;

    // 确保 chunk 在活跃列表中
    if (std::find(active_chunks_.begin(), active_chunks_.end(), chunk_idx) == active_chunks_.end())
        active_chunks_.push_back(chunk_idx);
}

void Scheduler::on_subblock_timeout(uint32_t chunk_idx, uint32_t begin) {
    // Fast Fail 超时：仅重置超时的那个 sub-block
    // 已完成的 sub-block 不受影响，chunk 保持在 DOWNLOADING 状态
    // 下一轮 tick() 会自动为该 sub-block 选择新 Peer 重发
    if (chunk_idx >= chunk_states_.size()) return;
    if (chunk_states_[chunk_idx] == ChunkState::COMPLETE) return;

    uint32_t slot = begin / SUB_BLOCK_SIZE;
    if (chunk_idx < chunk_sub_req_.size() && slot < chunk_sub_req_[chunk_idx].size()) {
        chunk_sub_req_[chunk_idx][slot] = false; // 标记为未请求，下一轮 tick 自动重发
    }
    if (chunk_idx < chunk_sub_req_time_.size() && slot < chunk_sub_req_time_[chunk_idx].size()) {
        chunk_sub_req_time_[chunk_idx][slot] = 0;
    }
}

void Scheduler::mark_all_complete(const std::vector<bool>& bitfield) {
    for (size_t i = 0; i < bitfield.size() && i < chunk_states_.size(); i++) {
        if (bitfield[i]) {
            if (chunk_states_[i] != ChunkState::COMPLETE) {
                if (chunk_states_[i] == ChunkState::MISSING) missing_count_--;
                chunk_states_[i] = ChunkState::COMPLETE;
                // 从活跃列表中移除
                auto it = std::find(active_chunks_.begin(), active_chunks_.end(), static_cast<uint32_t>(i));
                if (it != active_chunks_.end()) {
                    *it = active_chunks_.back();
                    active_chunks_.pop_back();
                }
            }
        }
    }
    if (missing_count_ == 0) phase_ = SchedulerPhase::DONE;
    else if (missing_count_ < ENDGAME_THRESHOLD) phase_ = SchedulerPhase::ENDGAME;
}

} // namespace thinbt
