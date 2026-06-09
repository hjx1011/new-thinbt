#ifndef THINBT_CHUNK_ASSEMBLER_HPP
#define THINBT_CHUNK_ASSEMBLER_HPP

#include "common/platform.hpp"
#include <atomic>
#include <vector>
#include <memory>
#include <functional>
#include <cstdint>

namespace thinbt {

static constexpr uint32_t SUB_BLOCK_SIZE = 16 * 1024;

struct ChunkCompleteMsg {
    uint32_t chunk_idx;
    uint32_t winning_peer_slot;
};

using ChunkCompleteCallback = std::function<void(ChunkCompleteMsg)>;

class ChunkAssembler {
public:
    ChunkAssembler() = default;

    // Initialize: point base at mmap'd region for this chunk
    void init(uint8_t* mmap_base, uint32_t chunk_size,
              uint32_t sub_block_size = SUB_BLOCK_SIZE);

    // Called by I/O thread pool when a Piece arrives
    // Returns true if this was the last sub-block (chunk complete)
    bool on_piece(uint32_t begin, const uint8_t* data, uint32_t len);

    // Check completion
    bool is_complete() const {
        return pending_count_.load(std::memory_order_acquire) == 0;
    }

    // Per-sub-block state for Endgame tracking
    struct SubBlock {
        uint32_t begin;
        uint32_t length;
    };
    const std::vector<SubBlock>& sub_blocks() const { return sub_blocks_; }

    const uint8_t* base() const { return base_; }
    uint32_t chunk_size() const { return chunk_size_; }
    uint32_t total_slots() const { return total_slots_; }
    uint32_t pending_count_val() const {
        return pending_count_.load(std::memory_order_acquire);
    }

private:
    uint8_t* base_ = nullptr;
    uint32_t chunk_size_ = 0;
    uint32_t sub_block_size_ = SUB_BLOCK_SIZE;
    uint32_t total_slots_ = 0;

    std::unique_ptr<std::atomic<uint32_t>[]> completed_mask_;
    uint32_t mask_words_ = 0;
    std::atomic<uint32_t> pending_count_{0};
    std::vector<SubBlock> sub_blocks_;
};

} // namespace thinbt
#endif
