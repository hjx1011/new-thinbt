#ifndef THINBT_FILE_READ_POOL_HPP
#define THINBT_FILE_READ_POOL_HPP

#include <functional>
#include "common/platform.hpp"
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>

namespace thinbt {

class FileReadPool {
public:
    using ReadCallback = std::function<void(ssize_t, std::shared_ptr<std::vector<uint8_t>>)>;
    static FileReadPool& instance();
    void start(size_t threads = 2);
    void stop();
    void post_read(int fd, off_t off, size_t len, ReadCallback cb);

private:
    FileReadPool();
    ~FileReadPool();
    void worker_loop();

    struct Task { int fd; off_t off; size_t len; ReadCallback cb; };

    std::vector<std::thread> threads_;
    std::deque<Task> q_;
    std::mutex mtx_;
    std::condition_variable cv_;
    bool running_ = false;
};

} // namespace thinbt

#endif
