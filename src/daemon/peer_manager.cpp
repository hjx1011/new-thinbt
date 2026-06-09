#include "peer_manager.hpp"
#include "scheduler.hpp"
#include <algorithm>
#include <random>
#include <cstring>
#include <iostream>
#include <arpa/inet.h>

namespace thinbt {

PeerManager::PeerManager(asio::io_context& io, Scheduler& sched, IOWorkerPool* io_pool,
                          const Sha1Digest& info_hash, uint32_t local_speed_mbps, uint16_t p2p_port)
    : io_(io), sched_(sched), io_pool_(io_pool), local_speed_mbps_(local_speed_mbps),
      acceptor_(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), p2p_port))
{
    memcpy(info_hash_.data(), info_hash.data(), 20);
}

void PeerManager::start_accept() {
    do_accept();
}

void PeerManager::do_accept() {
    auto& mgr = *this;
    acceptor_.async_accept(
        [&mgr](asio::error_code ec, asio::ip::tcp::socket sock) {
            if (!ec) {
                auto sess = std::make_shared<PeerSession>(mgr.io_, mgr.info_hash_, mgr.local_speed_mbps_);
                sess->set_scheduler(&mgr.sched_);
                sess->set_io_pool(mgr.io_pool_);
                sess->set_file_fd(mgr.file_fd_);
                sess->start_inbound(std::move(sock),
                    [&mgr](std::shared_ptr<PeerSession> s) { mgr.on_peer_disconnected(std::move(s)); });
                mgr.on_peer_connected(std::move(sess));
            }
            mgr.do_accept();
        });
}

void PeerManager::connect_to(const std::string& ip, uint16_t port, uint8_t /*flags*/) {
    auto sess = std::make_shared<PeerSession>(io_, info_hash_, local_speed_mbps_);
    sess->set_scheduler(&sched_);
    sess->set_io_pool(io_pool_);
    sess->set_file_fd(file_fd_);
    sess->start_outbound(ip, port,
        [this](std::shared_ptr<PeerSession> s) { on_peer_disconnected(std::move(s)); });
    on_peer_connected(std::move(sess));
}

void PeerManager::on_peer_connected(std::shared_ptr<PeerSession> sess) {
    uint32_t id = next_slot_id_++;
    sess->set_slot_id(id);
    sessions_.push_back(sess);
    sched_.on_peer_added(id, sess->link_speed_reported());
    recent_connects_[sess->remote_ip()] = std::chrono::steady_clock::now();
}

void PeerManager::on_peer_disconnected(std::shared_ptr<PeerSession> sess) {
    sched_.on_peer_removed(sess->slot_id());
    recent_disconnects_[sess->remote_ip()] = std::chrono::steady_clock::now();
    sessions_.erase(std::remove(sessions_.begin(), sessions_.end(), sess), sessions_.end());
}

// ── Choke (10s) ──
void PeerManager::tick_choke() {
    if (sessions_.empty()) return;

    std::vector<std::shared_ptr<PeerSession>> sorted = sessions_;
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) { return a->pipeline_cap() > b->pipeline_cap(); });

    uint32_t slots = std::min(4u + local_speed_mbps_ / 100 * 2, 20u);
    uint32_t tit_for_tat    = slots * 50 / 100;
    uint32_t optimistic     = slots * 25 / 100;
    uint32_t anti_starvation = slots - tit_for_tat - optimistic;

    for (uint32_t i = 0; i < std::min(tit_for_tat, (uint32_t)sorted.size()); i++)
        sorted[i]->set_choked(false);

    std::mt19937 rng(std::random_device{}());
    std::shuffle(sorted.begin(), sorted.end(), rng);
    for (uint32_t i = 0; i < std::min(optimistic, (uint32_t)sorted.size()); i++)
        sorted[i]->set_choked(false);

    for (auto& s : sorted) {
        if (anti_starvation == 0) break;
        if (s->link_speed_reported() < 1000 && s->is_choked()) {
            s->set_choked(false);
            anti_starvation--;
        }
    }

    // Send choke/unchoke messages
    for (auto& s : sessions_) {
        std::vector<uint8_t> msg(5);
        if (s->is_choked()) {
            uint32_t len_be = hton32(1); msg[4] = 0;
            memcpy(msg.data(), &len_be, 4);
        } else {
            uint32_t len_be = hton32(1); msg[4] = 1;
            memcpy(msg.data(), &len_be, 4);
        }
        s->send_message(std::move(msg));
    }
}

// ── PEX Delta (60s) ──
void PeerManager::tick_pex() {
    std::vector<PexPeer> delta;
    auto cutoff = std::chrono::steady_clock::now() - std::chrono::seconds(60);

    for (auto& [ip, t] : recent_connects_)
        if (t > cutoff) {
            PexPeer p{};
            p.ip   = inet_addr(ip.c_str());
            p.port = htons(16889);
            p.flags = 0;
            delta.push_back(p);
        }

    for (auto& [ip, t] : recent_disconnects_)
        if (t > cutoff) {
            PexPeer p{};
            p.ip   = inet_addr(ip.c_str());
            p.port = htons(16889);
            p.flags |= 0x80; // preserve original flags, set "left" bit
            delta.push_back(p);
        }

    if (!delta.empty()) {
        auto pex_msg = build_pex(true, delta);
        for (auto& s : sessions_) s->send_message(pex_msg);
    }

    recent_connects_.clear();
    recent_disconnects_.clear();
}

} // namespace thinbt
