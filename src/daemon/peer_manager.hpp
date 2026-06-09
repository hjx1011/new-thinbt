#ifndef THINBT_PEER_MANAGER_HPP
#define THINBT_PEER_MANAGER_HPP

#include "peer_session.hpp"
#include "protocol.hpp"
#include <asio.hpp>
#include <memory>
#include <vector>
#include <map>
#include <chrono>
#include <string>
#include <cstdint>

namespace thinbt {

class Scheduler;
class IOWorkerPool;

class PeerManager {
public:
    PeerManager(asio::io_context& io, Scheduler& sched, IOWorkerPool* io_pool,
                const Sha1Digest& info_hash, uint32_t local_speed_mbps, uint16_t p2p_port);

    void start_accept();
    void connect_to(const std::string& ip, uint16_t port, uint8_t flags);

    void tick_choke();
    void tick_pex();

    size_t peer_count() const { return sessions_.size(); }
    void set_file_fd(int fd) { file_fd_ = fd; }

private:
    void on_peer_connected(std::shared_ptr<PeerSession> sess);
    void on_peer_disconnected(std::shared_ptr<PeerSession> sess);
    void do_accept();

    asio::io_context& io_;
    Scheduler& sched_;
    IOWorkerPool* io_pool_;
    Sha1Digest info_hash_;
    uint32_t local_speed_mbps_;
    asio::ip::tcp::acceptor acceptor_;
    int file_fd_ = -1;
    uint32_t next_slot_id_ = 0;
    std::vector<std::shared_ptr<PeerSession>> sessions_;

    std::map<std::string, std::chrono::steady_clock::time_point> recent_connects_;
    std::map<std::string, std::chrono::steady_clock::time_point> recent_disconnects_;
};

} // namespace thinbt
#endif
