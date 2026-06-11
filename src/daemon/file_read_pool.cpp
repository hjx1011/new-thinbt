#include "file_read_pool.hpp"
#include "common/platform.hpp"
#include <cassert>

namespace thinbt {

FileReadPool& FileReadPool::instance() {
    static FileReadPool inst;
    return inst;
}

FileReadPool::FileReadPool() {}
FileReadPool::~FileReadPool() { stop(); }

void FileReadPool::start(size_t threads) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (running_) return;
    running_ = true;
    threads_.reserve(threads);
    for (size_t i = 0; i < threads; ++i) threads_.emplace_back(&FileReadPool::worker_loop, this);
}

void FileReadPool::stop() {
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

void FileReadPool::post_read(int fd, off_t off, size_t len, ReadCallback cb) {
    Task tk; tk.fd = fd; tk.off = off; tk.len = len; tk.cb = std::move(cb);
    {
        std::lock_guard<std::mutex> lk(mtx_);
        q_.push_back(std::move(tk));
    }
    cv_.notify_one();
}

void FileReadPool::worker_loop() {
    while (true) {
        Task tk;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [this]{ return !q_.empty() || !running_; });
            if (!running_ && q_.empty()) return;
            tk = std::move(q_.front()); q_.pop_front();
        }

        auto buf = std::make_shared<std::vector<uint8_t>>(tk.len);
        ssize_t n = thinbt_pread(tk.fd, buf->data(), tk.len, tk.off);
        if (n <= 0) {
            buf->clear();
        } else if (static_cast<size_t>(n) < tk.len) {
            buf->resize(static_cast<size_t>(n));
        }

        try {
            if (tk.cb) tk.cb(n, buf);
        } catch(...) { }
    }
}

} // namespace thinbt
