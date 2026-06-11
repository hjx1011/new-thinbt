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
    static constexpr size_t MAX_PEERS = 60;

    PeerManager(asio::io_context& io, Scheduler& sched, IOWorkerPool* io_pool,
                const Sha1Digest& info_hash, uint32_t local_speed_mbps, uint16_t p2p_port);
    ~PeerManager();

    void start_accept();
    void connect_to(const std::string& ip, uint16_t port, uint8_t flags);

    void tick_choke();
    void tick_pex();

    size_t peer_count() const { return sessions_.size(); }
    void set_file_fd(int fd) { file_fd_ = fd; }
    void set_initial_bitfield(const std::vector<bool>& bf) { initial_bitfield_ = bf; }
    void set_chunk_offsets(const std::vector<uint64_t>* offsets) { chunk_offsets_ = offsets; }

    PeerSession* get_session(uint32_t slot_id);
    const std::vector<std::shared_ptr<PeerSession>>& sessions() const { return sessions_; }

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
    uint16_t p2p_port_;
    int file_fd_ = -1;
    std::vector<bool> initial_bitfield_;
    const std::vector<uint64_t>* chunk_offsets_ = nullptr;
    uint32_t next_slot_id_ = 0;
    std::vector<std::shared_ptr<PeerSession>> sessions_;

    std::map<std::string, std::pair<std::chrono::steady_clock::time_point, uint8_t>> recent_connects_;
    std::map<std::string, std::pair<std::chrono::steady_clock::time_point, uint8_t>> recent_disconnects_;
};

} // namespace thinbt
#endif
