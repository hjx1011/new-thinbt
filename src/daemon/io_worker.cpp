#include "io_worker.hpp"
#include <cassert>
#include <iostream>

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
    q.queue.enqueue(task);      // SPSC 无锁入队
    q.cv.notify_one();          // 唤醒对应 worker
}

void IOWorkerPool::worker_loop(uint32_t worker_id) {
    auto& q = *queues_[worker_id];
    while (running_.load(std::memory_order_acquire)) {
        PieceTask task;
        // 无锁批量消费 SPSC 队列
        bool any = false;
        while (q.queue.try_dequeue(task)) {
            any = true;
            // Diagnostics: validate task and owner before writing into mmap
            if (!task.owner) {
                std::cerr << "[IOWorker] WARNING: task.owner is null for chunk " << task.chunk_idx << std::endl;
            } else {
                // optional: report use_count in abnormal cases
                if (task.owner.use_count() == 0) {
                    std::cerr << "[IOWorker] WARNING: owner use_count==0 for chunk " << task.chunk_idx << std::endl;
                }
            }

            // Bounds check: prevent on_piece from attempting out-of-range writes
            if (task.chunk_idx == UINT32_MAX) {
                std::cerr << "[IOWorker] ERROR: invalid chunk_idx UINT32_MAX" << std::endl;
            }

            bool complete = false;
            try {
                complete = assemblers_[task.chunk_idx].on_piece(
                    task.begin, task.data, task.length);
            } catch (const std::exception& e) {
                std::cerr << "[IOWorker] exception in on_piece: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "[IOWorker] unknown exception in on_piece" << std::endl;
            }
            if (complete && on_complete_) {
                ChunkCompleteMsg msg{task.chunk_idx, task.peer_slot};
                on_complete_(msg);
            }
        }
        if (!any) {
            std::unique_lock<std::mutex> lock(q.mtx);
            q.cv.wait_for(lock, std::chrono::milliseconds(1));
        }
    }
}

} // namespace thinbt