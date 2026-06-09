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
                      RequestIssuer issue_req, HaveBroadcaster broadcast_have) {
    chunk_states_.resize(total_chunks, ChunkState::MISSING);
    availability_.resize(total_chunks, 0);
    chunk_requested_end_.resize(total_chunks, 0);
    chunk_sub_done_.resize(total_chunks);
    chunk_first_req_time_.resize(total_chunks, 0);
    missing_count_ = total_chunks;
    local_speed_mbps_ = local_speed_mbps;
    issue_request_  = std::move(issue_req);
    broadcast_have_ = std::move(broadcast_have);
}

void Scheduler::set_cancel_issuer(CancelIssuer cancel) {
    cancel_request_ = std::move(cancel);
}

void Scheduler::set_chunk_sizes(const std::vector<uint32_t>& sizes) {
    chunk_sub_blocks_.resize(sizes.size());
    for (size_t i = 0; i < sizes.size(); i++) {
        uint32_t n = (sizes[i] + SUB_BLOCK_SIZE - 1) / SUB_BLOCK_SIZE;
        chunk_sub_blocks_[i] = n;
        if (i < chunk_sub_done_.size())
            chunk_sub_done_[i].resize(n, false);
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

    // 标记 sub-block 完成位图
    uint32_t slot = begin / SUB_BLOCK_SIZE;
    if (chunk_idx < chunk_sub_done_.size() && slot < chunk_sub_done_[chunk_idx].size())
        chunk_sub_done_[chunk_idx][slot] = true;

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
    // NORMAL 逻辑（所有阶段都执行）：Rarest First + 批量发 sub-block
    // ═══════════════════════════════════════════════════════
    std::vector<std::pair<uint32_t, uint32_t>> pending;
    for (uint32_t i = 0; i < chunk_states_.size(); i++) {
        uint32_t total = i < chunk_sub_blocks_.size() ? chunk_sub_blocks_[i] : 8;
        uint32_t done  = i < chunk_requested_end_.size() ? chunk_requested_end_[i] : 0;
        if (chunk_states_[i] == ChunkState::COMPLETE) continue;
        if (done >= total) {
            // 所有 sub-block 已发出但尚未全部完成 → 保持在 DOWNLOADING
            if (chunk_states_[i] == ChunkState::MISSING) continue;
            continue;
        }
        pending.emplace_back(availability_[i], i);
    }
    if (!pending.empty()) {
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

            uint32_t remaining = total - done;
            uint32_t to_issue = std::min(cap - p->pending_requests, remaining);
            if (to_issue == 0) continue;

            // 首次发出 sub-block → 进入 DOWNLOADING，记录时间戳
            bool first_request = (chunk_states_[ci] == ChunkState::MISSING);
            chunk_states_[ci] = ChunkState::DOWNLOADING;
            if (first_request && ci < chunk_first_req_time_.size())
                chunk_first_req_time_[ci] = now;

            for (uint32_t b = 0; b < to_issue; b++) {
                uint32_t begin = (done + b) * SUB_BLOCK_SIZE;
                uint32_t len   = SUB_BLOCK_SIZE;
                issue_request_(peer, ci, begin, len);
                p->pending_requests++;
                p->active_sub_blocks[ci].insert(begin);
            }
            chunk_requested_end_[ci] = done + to_issue;
        }
    }

    // ═══════════════════════════════════════════════════════
    // ENDGAME 精准补漏逻辑
    // ═══════════════════════════════════════════════════════
    if (phase_ == SchedulerPhase::ENDGAME)
        tick_endgame(now);
}

void Scheduler::tick_endgame(uint64_t now_ms) {
    // 收集所有 DOWNLOADING 状态的 chunk
    std::vector<uint32_t> downloading;
    for (uint32_t i = 0; i < chunk_states_.size(); i++) {
        if (chunk_states_[i] == ChunkState::DOWNLOADING)
            downloading.push_back(i);
    }
    if (downloading.empty()) return;

    // 优先处理剩余 sub-block 少的 chunk（接近完成的最需要加速）
    std::sort(downloading.begin(), downloading.end(), [this](uint32_t a, uint32_t b) {
        uint32_t rem_a = 0, rem_b = 0;
        if (a < chunk_sub_blocks_.size() && a < chunk_sub_done_.size()) {
            uint32_t total = chunk_sub_blocks_[a];
            uint32_t done  = static_cast<uint32_t>(
                std::count(chunk_sub_done_[a].begin(), chunk_sub_done_[a].end(), true));
            rem_a = total > done ? total - done : 0;
        }
        if (b < chunk_sub_blocks_.size() && b < chunk_sub_done_.size()) {
            uint32_t total = chunk_sub_blocks_[b];
            uint32_t done  = static_cast<uint32_t>(
                std::count(chunk_sub_done_[b].begin(), chunk_sub_done_[b].end(), true));
            rem_b = total > done ? total - done : 0;
        }
        return rem_a < rem_b;  // 剩余少的优先
    });

    uint32_t endgame_processed = 0;
    for (uint32_t ci : downloading) {
        if (endgame_processed >= MAX_ENDGAME_CHUNKS) break;

        uint32_t total = ci < chunk_sub_blocks_.size() ? chunk_sub_blocks_[ci] : 0;
        if (total == 0 || ci >= chunk_sub_done_.size()) continue;

        // 饥饿判定阈值：2 倍当前负责 Peer 的 RTT，至少 50ms
        uint64_t chunk_req_time = ci < chunk_first_req_time_.size()
            ? chunk_first_req_time_[ci] : 0;
        if (chunk_req_time == 0) continue;

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

        // 计算饥饿阈值
        uint64_t hunger_threshold_ms = 50; // 最小 50ms
        if (primary_peer != UINT32_MAX) {
            auto* pp = peer_slot(primary_peer);
            if (pp && pp->rtt_us > 0)
                hunger_threshold_ms = std::max<uint64_t>(pp->rtt_us * 2 / 1000, 50);
        }

        bool any_hungry = false;
        for (uint32_t s = 0; s < total; s++) {
            if (chunk_sub_done_[ci][s]) continue;
            if ((now_ms - chunk_req_time) <= hunger_threshold_ms) continue;

            // 饥饿 sub-block：找 2 个最空闲的额外 Peer 发冗余请求
            any_hungry = true;
            auto redundant = find_redundant_peers(ci, primary_peer, MAX_REDUNDANT_PEERS, peer_slots_);
            for (uint32_t rp : redundant) {
                auto* rpeer = peer_slot(rp);
                if (!rpeer) continue;
                uint32_t begin = s * SUB_BLOCK_SIZE;
                uint32_t len   = SUB_BLOCK_SIZE;
                issue_request_(rp, ci, begin, len);
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

        // Cancel 清理：chunk 已完成，向所有还在传该 chunk 的 peer 发 Cancel
        // 冗余请求的数据已经无用了，必须尽快取消以释放对端上行带宽和本端 I/O
        send_cancel_for_chunk(msg.chunk_idx, UINT32_MAX); // 取消所有 peer，不排除任何

        broadcast_have_(msg.chunk_idx);
    }
    completions.clear();
    if (missing_count_ == 0) phase_ = SchedulerPhase::DONE;
    else if (missing_count_ < ENDGAME_THRESHOLD) phase_ = SchedulerPhase::ENDGAME;
}

void Scheduler::on_verify_failed(uint32_t chunk_idx) {
    // SHA-256 校验失败：回退为 MISSING，Scheduler 下一轮 tick() 自动重新下载
    chunk_states_[chunk_idx] = ChunkState::MISSING;
    if (chunk_idx < chunk_requested_end_.size())
        chunk_requested_end_[chunk_idx] = 0;
    if (chunk_idx < chunk_sub_done_.size())
        std::fill(chunk_sub_done_[chunk_idx].begin(), chunk_sub_done_[chunk_idx].end(), false);
}

void Scheduler::on_subblock_timeout(uint32_t chunk_idx) {
    // Fast Fail 超时：Peer 未能在 3 秒内交付子块
    // 将 chunk 重置为 MISSING，下一轮 tick() 重新分配新 Peer
    // ChunkAssembler 是幂等的（mmap 原地写入），已收到子块重复下载不影响正确性
    if (chunk_idx >= chunk_states_.size()) return;
    if (chunk_states_[chunk_idx] == ChunkState::COMPLETE) return;
    chunk_states_[chunk_idx] = ChunkState::MISSING;
    if (chunk_idx < chunk_requested_end_.size())
        chunk_requested_end_[chunk_idx] = 0;
    if (chunk_idx < chunk_sub_done_.size())
        std::fill(chunk_sub_done_[chunk_idx].begin(), chunk_sub_done_[chunk_idx].end(), false);
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

} // namespace thinbt
