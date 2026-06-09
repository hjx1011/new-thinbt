#include "verify_worker.hpp"
#include <cstring>
#include <iostream>

namespace thinbt {

VerifyWorkerPool::~VerifyWorkerPool() { stop(); }

void VerifyWorkerPool::start(const ChunkEntry* chunks,
                              const ChunkAssembler* assemblers,
                              uint32_t num_workers) {
    chunks_     = chunks;
    assemblers_ = assemblers;

    if (num_workers == 0) {
        // SHA-256 是纯 CPU 计算，线程太多会与 I/O 线程争核
        // 取 1/4 核数，至少 2 最多 4
        uint32_t n = std::thread::hardware_concurrency();
        num_workers = std::max(2u, std::min(4u, n / 4));
    }
    num_workers_ = num_workers;

    running_.store(true, std::memory_order_release);

    threads_.reserve(num_workers_);
    for (uint32_t i = 0; i < num_workers_; i++)
        threads_.emplace_back(&VerifyWorkerPool::worker_loop, this);
}

void VerifyWorkerPool::stop() {
    running_.store(false, std::memory_order_release);
    // 向 MPMC 队列塞入 num_workers_ 个空任务作为毒丸
    // ConcurrentQueue 不支持"唤醒阻塞消费者"的机制，
    // 用空任务（chunk_idx == UINT32_MAX）作为退出信号
    VerifyTask poison{};
    poison.chunk_idx = UINT32_MAX;
    for (uint32_t i = 0; i < num_workers_; i++)
        task_queue_.enqueue(poison);

    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
}

void VerifyWorkerPool::enqueue(ChunkCompleteMsg msg) {
    const auto& asm_ = assemblers_[msg.chunk_idx];
    const auto& ch   = chunks_[msg.chunk_idx];

    VerifyTask task{};
    task.chunk_idx          = msg.chunk_idx;
    task.winning_peer_slot  = msg.winning_peer_slot;
    task.data               = asm_.base();
    task.length             = asm_.chunk_size();
    task.expected_hash      = ch.sha256;  // uint8_t[32]，直接指向 seed 内存

    task_queue_.enqueue(task);
}

void VerifyWorkerPool::drain_results(std::vector<VerifyResult>& out) {
    VerifyResult r;
    while (result_queue_.try_dequeue(r))
        out.push_back(r);
}

void VerifyWorkerPool::worker_loop() {
    VerifyTask task;
    while (running_.load(std::memory_order_acquire)) {
        // ConcurrentQueue::try_dequeue 是非阻塞的
        // 忙等轮询（计算线程的典型模式：低延迟优先于省电）
        if (task_queue_.try_dequeue(task)) {
            // 毒丸检查
            if (task.chunk_idx == UINT32_MAX)
                break;

            // SHA-256 计算——这是独立线程池的原因：
            // 50GB 文件按 128KB chunk ≈ 40 万个 chunk，
            // 每个 chunk SHA-256 ~640μs，在 I/O 线程中做会阻塞 memcpy，
            // 在网络线程中做会导致 epoll 饥饿
            Sha256Digest actual = sha256(task.data, task.length);

            // 常量时间比较，防时序攻击（局域网场景非必需但无成本）
            bool passed = (memcmp(actual.data(), task.expected_hash, 32) == 0);

            if (!passed) {
                std::cerr << "[verify] chunk=" << task.chunk_idx
                          << " SHA-256 mismatch"
                          << " expected=" << sha256_hex(
                                 *reinterpret_cast<const Sha256Digest*>(task.expected_hash))
                          << " actual=" << sha256_hex(actual)
                          << std::endl;
            }

            VerifyResult result{};
            result.chunk_idx         = task.chunk_idx;
            result.winning_peer_slot = task.winning_peer_slot;
            result.passed            = passed;
            result.actual_hash       = actual;

            // SPSC 写回主线程——只有计算线程生产，主线程消费
            result_queue_.enqueue(result);
        }
        // 不 sleep：计算线程应该全速运行，SHA-256 是瓶颈
        // 如果队列空，CAS 重试本身就是"等待"
    }
}

} // namespace thinbt
