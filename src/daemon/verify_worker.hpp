#ifndef THINBT_VERIFY_WORKER_HPP
#define THINBT_VERIFY_WORKER_HPP

#include "common/platform.hpp"
#include "common/hash.hpp"
#include "chunk_assembler.hpp"
#include "seed/tseed.hpp"
#include "concurrentqueue.h"
#include "readerwriterqueue.h"
#include <vector>
#include <thread>
#include <atomic>
#include <functional>
#include <memory>

namespace thinbt {

// I/O 线程池 → 计算线程池的校验任务
struct VerifyTask {
    uint32_t chunk_idx;
    uint32_t winning_peer_slot;
    const uint8_t* data;          // 指向 mmap 中的 Chunk 数据（只读）
    uint32_t length;
    const uint8_t* expected_hash; // 指向 ChunkEntry::sha256，32 字节
};

// 计算线程池 → 主线程的校验结果
struct VerifyResult {
    uint32_t chunk_idx;
    uint32_t winning_peer_slot;
    bool     passed;
    Sha256Digest actual_hash;
};

// 校验完成后的回调类型
using VerifyCallback = std::function<void(VerifyResult)>;

class VerifyWorkerPool {
public:
    VerifyWorkerPool() = default;
    ~VerifyWorkerPool();

    // chunks: ChunkEntry 数组（只读，取 expected hash）
    // assemblers: ChunkAssembler 数组（只读，取 mmap base 指针）
    // num_workers: 计算线程数，0 表示自动选择（2~4）
    void start(const ChunkEntry* chunks,
               const ChunkAssembler* assemblers,
               uint32_t num_workers = 0);

    void stop();

    // I/O 线程调用：多个 I/O 线程并发入队（MPMC 无锁）
    void enqueue(ChunkCompleteMsg msg);

    // 主线程调用：批量取出所有已完成校验的结果
    void drain_results(std::vector<VerifyResult>& out);

    uint32_t worker_count() const { return num_workers_; }

private:
    void worker_loop();

    const ChunkEntry* chunks_ = nullptr;
    const ChunkAssembler* assemblers_ = nullptr;

    // MPMC 无锁队列：多个 I/O 线程生产，多个计算线程消费
    moodycamel::ConcurrentQueue<VerifyTask> task_queue_;

    // SPSC 无锁队列：计算线程生产，主线程消费
    moodycamel::ReaderWriterQueue<VerifyResult> result_queue_{4096};

    std::atomic<bool> running_{false};
    uint32_t num_workers_ = 0;
    std::vector<std::thread> threads_;
};

} // namespace thinbt
#endif
