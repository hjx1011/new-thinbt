#include "sendfile_pool.hpp"
#include <cassert>

namespace thinbt {

SendfilePool& SendfilePool::instance() {
    static SendfilePool inst;
    return inst;
}

SendfilePool::SendfilePool() {}
SendfilePool::~SendfilePool() { stop(); }

void SendfilePool::start(size_t threads) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (running_) return;
    running_ = true;
    threads_.reserve(threads);
    for (size_t i = 0; i < threads; ++i) threads_.emplace_back(&SendfilePool::worker_loop, this);
}

void SendfilePool::stop() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_all();
    for (auto& t : threads_) if (t.joinable()) t.join();
    threads_.clear();
    std::lock_guard<std::mutex> lk(mtx_);
    q_.clear();
}

void SendfilePool::post(std::function<void()> job) {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        q_.push_back(std::move(job));
    }
    cv_.notify_one();
}

void SendfilePool::worker_loop() {
    while (true) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [this]{ return !q_.empty() || !running_; });
            if (!running_ && q_.empty()) return;
            job = std::move(q_.front()); q_.pop_front();
        }
        try { job(); } catch(...) { }
    }
}

} // namespace thinbt
