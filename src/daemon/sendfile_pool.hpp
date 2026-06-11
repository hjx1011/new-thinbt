#ifndef THINBT_SENDFILE_POOL_HPP
#define THINBT_SENDFILE_POOL_HPP

#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>

namespace thinbt {

class SendfilePool {
public:
    static SendfilePool& instance();
    void start(size_t threads = 4);
    void stop();
    void post(std::function<void()> job);

private:
    SendfilePool();
    ~SendfilePool();

    void worker_loop();

    std::vector<std::thread> threads_;
    std::deque<std::function<void()>> q_;
    std::mutex mtx_;
    std::condition_variable cv_;
    bool running_ = false;
};

} // namespace thinbt

#endif
