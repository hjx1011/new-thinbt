#ifndef THINBT_IO_WORKER_HPP
#define THINBT_IO_WORKER_HPP

#include "common/platform.hpp"
#include "chunk_assembler.hpp"
#include "readerwriterqueue.h"
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <memory>
#include <atomic>
#include <functional>

namespace thinbt {

struct PieceTask {
    uint32_t chunk_idx;
    uint32_t begin;
    uint32_t length;
    uint32_t peer_slot;                            // 哪个 Peer 在处理这个 Piece
    const uint8_t* data;
    std::shared_ptr<std::vector<uint8_t>> owner;   // 持有数据所有权，零额外拷贝
};

class IOWorkerPool {
public:
    IOWorkerPool() = default;
    ~IOWorkerPool();

    void start(uint32_t num_workers,
               ChunkAssembler* assemblers,
               ChunkCompleteCallback on_complete);

    void stop();

    // Called by network thread. Routes: (chunk_idx ^ slot_idx) % N
    void dispatch(PieceTask task);

    uint32_t worker_count() const { return num_workers_; }

private:
    void worker_loop(uint32_t worker_id);

    struct WorkerQueue {
        moodycamel::ReaderWriterQueue<PieceTask> queue{4096};
        std::mutex mtx;
        std::condition_variable cv;
    };

    uint32_t num_workers_ = 0;
    ChunkAssembler* assemblers_ = nullptr;
    ChunkCompleteCallback on_complete_;
    std::atomic<bool> running_{false};

    std::vector<std::unique_ptr<WorkerQueue>> queues_;
    std::vector<std::thread> threads_;
};

} // namespace thinbt
#endif