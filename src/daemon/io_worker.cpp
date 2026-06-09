#include "io_worker.hpp"
#include <cassert>

namespace thinbt {

IOWorkerPool::~IOWorkerPool() { stop(); }

void IOWorkerPool::start(uint32_t num_workers,
                          ChunkAssembler* assemblers,
                          ChunkCompleteCallback on_complete) {
    assert(num_workers > 0 && num_workers <= 32);
    num_workers_ = num_workers;
    assemblers_  = assemblers;
    on_complete_ = std::move(on_complete);

    queues_.reserve(num_workers);
    threads_.reserve(num_workers);

    running_.store(true, std::memory_order_release);

    for (uint32_t i = 0; i < num_workers; i++) {
        queues_.push_back(std::make_unique<WorkerQueue>());
        threads_.emplace_back(&IOWorkerPool::worker_loop, this, i);
    }
}

void IOWorkerPool::stop() {
    running_.store(false, std::memory_order_release);
    // 唤醒所有等待的线程，让它们检查 running_ 并退出
    for (auto& q : queues_) {
        if (q) q->cv.notify_all();
    }
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
    queues_.clear();
}

void IOWorkerPool::dispatch(PieceTask task) {
    uint32_t slot_idx = task.begin / SUB_BLOCK_SIZE;
    uint32_t worker   = (task.chunk_idx ^ slot_idx) % num_workers_;
    auto& q = *queues_[worker];
    {
        std::lock_guard<std::mutex> lock(q.mtx);
        q.tasks.push_back(task);
    }
    q.cv.notify_one(); // 唤醒对应 worker
}

void IOWorkerPool::worker_loop(uint32_t worker_id) {
    auto& q = *queues_[worker_id];
    while (running_.load(std::memory_order_acquire)) {
        PieceTask task;
        bool has_task = false;
        {
            std::unique_lock<std::mutex> lock(q.mtx);
            // 条件变量挂起，而非 yield 空转
            q.cv.wait(lock, [&] {
                return !q.tasks.empty() || !running_.load(std::memory_order_acquire);
            });
            if (!q.tasks.empty()) {
                task = q.tasks.front();
                q.tasks.pop_front();
                has_task = true;
            }
        }
        if (has_task) {
            bool complete = assemblers_[task.chunk_idx].on_piece(
                task.begin, task.data, task.length);
            if (complete && on_complete_) {
                ChunkCompleteMsg msg{task.chunk_idx, 0};
                on_complete_(msg);
            }
        }
    }
}

} // namespace thinbt
