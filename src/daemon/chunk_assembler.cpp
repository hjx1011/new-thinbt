#include "chunk_assembler.hpp"
#include <cstring>
#include <algorithm>
#include <iostream>

namespace thinbt {

void ChunkAssembler::init(uint8_t* mmap_base, uint32_t chunk_size, uint32_t sub_block_size) {
    base_ = mmap_base;
    chunk_size_ = chunk_size;
    sub_block_size_ = sub_block_size;

    std::cerr << "[ChunkAssembler] init base=" << static_cast<const void*>(base_)
              << " chunk_size=" << chunk_size_ << " sub_block_size=" << sub_block_size_ << std::endl;

    total_slots_ = (chunk_size + sub_block_size - 1) / sub_block_size;
    mask_words_ = (total_slots_ + 31) / 32;
    completed_mask_.reset(new std::atomic<uint32_t>[mask_words_]());
    // Elements are value-initialized to 0 by the () above
    pending_count_.store(total_slots_, std::memory_order_relaxed);

    sub_blocks_.resize(total_slots_);
    for (uint32_t i = 0; i < total_slots_; i++) {
        sub_blocks_[i].begin  = i * sub_block_size;
        sub_blocks_[i].length = std::min(sub_block_size, chunk_size - sub_blocks_[i].begin);
    }
}

bool ChunkAssembler::on_piece(uint32_t begin, const uint8_t* data, uint32_t len) {
    uint32_t slot     = begin / sub_block_size_;
    uint32_t word     = slot / 32;
    uint32_t bit_mask = 1u << (slot % 32);

    // Defensive checks: ensure base_ and ranges are valid to avoid SIGBUS
    if (!base_) {
        std::cerr << "[ChunkAssembler] ERROR: base_ is null" << std::endl;
        return false;
    }
    if (begin + len > chunk_size_) {
        std::cerr << "[ChunkAssembler] ERROR: write out-of-bounds begin=" << begin
                  << " len=" << len << " chunk_size=" << chunk_size_ << std::endl;
        return false;
    }

    if (len > sub_block_size_ || (begin % sub_block_size_) != 0) {
        std::cerr << "[ChunkAssembler] WARN: unusual piece begin=" << begin
                  << " len=" << len << " sub_block_size=" << sub_block_size_ << std::endl;
    }

    // Write to mmap region (pure memory operation, nanosecond-scale)
    memcpy(base_ + begin, data, len);

    // Atomic check-and-mark: only first writer for this slot decrements counter
    uint32_t old_mask = completed_mask_[word].fetch_or(bit_mask, std::memory_order_release);

    if ((old_mask & bit_mask) == 0) {
        // First completion of this slot — safe to decrement
        if (pending_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            return true; // Chunk complete
        }
    }
    // else: stale duplicate from a timed-out peer, data overwritten, counter untouched
    return false;
}

} // namespace thinbt
