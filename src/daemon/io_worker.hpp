#ifndef THINBT_IO_WORKER_HPP
#define THINBT_IO_WORKER_HPP

#include "common/platform.hpp"
#include "chunk_assembler.hpp"
#include <vector>
#include <deque>
#include <mutex>
#include <thread>
#include <memory>
#include <atomic>
#include <functional>

namespace thinbt {

struct PieceTask {
    uint32_t chunk_idx;
    uint32_t begin;
    uint32_t length;
    const uint8_t* data;
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
        std::deque<PieceTask> tasks;
        std::mutex mtx;
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
