#ifndef THINBT_SCHEDULER_HPP
#define THINBT_SCHEDULER_HPP

#include "common/platform.hpp"
#include "chunk_assembler.hpp"
#include "protocol.hpp"
#include <vector>
#include <functional>
#include <map>
#include <set>
#include <optional>
#include <cstdint>

namespace thinbt {

struct PeerSlot {
    uint32_t slot_id = 0;
    uint32_t pending_requests = 0;
    uint32_t consecutive_timeouts = 0;
    uint32_t rtt_us = 0;
    uint32_t link_speed_mbps = 0;
    uint32_t link_speed_reported = 0;
    uint32_t pipeline_cap = 128;
    bool     am_choking = true;
    std::vector<bool> remote_bitfield;

    // chunk_idx → set of begin offsets this peer is currently downloading
    // 用于 Chunk 完成时 Cancel 清理 + 饥饿判定时排除已有请求的 Peer
    std::map<uint32_t, std::set<uint32_t>> active_sub_blocks;
};

enum class SchedulerPhase { NORMAL, ENDGAME, DONE };
enum class ChunkState : uint8_t { MISSING, REQUESTED, DOWNLOADING, COMPLETE };

class Scheduler {
public:
    using RequestIssuer = std::function<void(uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length)>;
    using HaveBroadcaster = std::function<void(uint32_t chunk_idx)>;
    using CancelIssuer = std::function<void(uint32_t peer_slot, uint32_t chunk_idx, uint32_t begin, uint32_t length)>;
    using NotInterestedBroadcaster = std::function<void()>;
    using NotInterestedIssuer = std::function<void(uint32_t peer_slot)>;

    void init(uint32_t total_chunks, uint32_t local_speed_mbps,
              RequestIssuer issue_req, HaveBroadcaster broadcast_have,
              NotInterestedBroadcaster broadcast_not_interested);
    void set_cancel_issuer(CancelIssuer cancel);
    void set_not_interested_issuer(NotInterestedIssuer issuer);
    void set_chunk_sizes(const std::vector<uint32_t>& sizes);

    // Event-driven availability (O(1) per event)
    void on_bitfield(uint32_t slot_id, const std::vector<bool>& bf);
    void on_have(uint32_t slot_id, uint32_t chunk_idx);
    void on_peer_added(uint32_t slot_id, uint32_t reported_speed);
    void on_peer_removed(uint32_t slot_id);
    void on_choke_change(uint32_t slot_id, bool choking);
    void dec_peer_pending(uint32_t slot_id, uint32_t chunk_idx, uint32_t begin);

    void tick();
    void process_completions(std::vector<ChunkCompleteMsg>& completions);
    void on_verify_failed(uint32_t chunk_idx);
    void on_subblock_timeout(uint32_t chunk_idx, uint32_t begin);
    void mark_all_complete(const std::vector<bool>& bitfield);

    SchedulerPhase phase() const { return phase_; }
    uint32_t missing_count() const { return missing_count_; }
    PeerSlot* peer_slot(uint32_t id);

private:
    std::optional<uint32_t> select_best_peer(uint32_t chunk_idx);
    void send_cancel_for_chunk(uint32_t chunk_idx, uint32_t exclude_slot);
    void tick_endgame(uint64_t now_ms);
    void check_peer_interest(uint32_t slot_id);  // 检查 peer 是否有我们需要的 chunk

    std::vector<ChunkState> chunk_states_;
    std::vector<uint32_t>  chunk_sub_blocks_;   // 每个 chunk 的 sub-block 数量
    std::vector<uint32_t>  availability_;
    std::vector<uint32_t>  active_chunks_;       // 非 COMPLETE 的 chunk 索引缓存，避免 O(N) 全扫

    // sub-block 级别跟踪（主线程独占，无需 atomic）
    // chunk_sub_done_[chunk][slot] = true 表示该 sub-block 已完成
    std::vector<std::vector<bool>> chunk_sub_done_;
    // chunk_sub_req_[chunk][slot] = true 表示该 sub-block 已发出请求
    std::vector<std::vector<bool>> chunk_sub_req_;
    // chunk_sub_req_time_[chunk][slot] = 发出请求时的单调时钟（毫秒），0 表示未请求
    std::vector<std::vector<uint64_t>> chunk_sub_req_time_;
    // chunk_sub_done_count_[chunk] = 该 chunk 已完成的 sub-block 数量
    std::vector<uint32_t> chunk_sub_done_count_;
    // chunk_sub_req_count_[chunk] = 该 chunk 已发出请求的 sub-block 数量（用于 O(1) 检测）
    std::vector<uint32_t> chunk_sub_req_count_;

    std::vector<PeerSlot>  peer_slots_;
    SchedulerPhase phase_ = SchedulerPhase::NORMAL;
    uint32_t missing_count_ = 0;
    uint32_t local_speed_mbps_ = 1000;
    RequestIssuer issue_request_;
    CancelIssuer  cancel_request_;
    HaveBroadcaster broadcast_have_;
    NotInterestedBroadcaster broadcast_not_interested_;
    NotInterestedIssuer not_interested_issuer_;

    static constexpr uint32_t ENDGAME_THRESHOLD    = 128;
    static constexpr uint32_t MAX_ENDGAME_CHUNKS   = 32;
    static constexpr uint32_t MAX_REDUNDANT_PEERS  = 2;   // 饥饿时最多额外发 2 个 Peer
};

} // namespace thinbt
#endif
