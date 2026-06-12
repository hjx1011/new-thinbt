#include "peer_manager.hpp"
#include "scheduler.hpp"
#include "common/net_util.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <random>

namespace thinbt {
namespace {

bool is_unspecified_or_broadcast(const std::string& ip) {
    return ip == "0.0.0.0" || ip == "255.255.255.255";
}

std::vector<PexPeer> make_full_pex_list(
    const std::vector<std::shared_ptr<PeerSession>>& sessions,
    const PeerSession* exclude)
{
    std::vector<PexPeer> peers;
    for (const auto& sess : sessions) {
        if (!sess || sess.get() == exclude) continue;
        std::string ip = sess->remote_ip();
        if (!is_connectable_peer_ip(ip)) continue;

        PexPeer p{};
        p.ip = inet_addr(ip.c_str());
        uint16_t listen_port = sess->remote_listen_port();
        p.port = htons(listen_port != 0 ? listen_port : sess->remote_port());
        p.flags = sess->link_speed_reported() >= 1000 ? 0x02 : 0x00;
        peers.push_back(p);
    }
    return peers;
}

} // namespace

bool is_connectable_peer_ip(const std::string& ip) {
    if (ip.empty() || ip == "unknown") return false;
    if (is_unspecified_or_broadcast(ip)) return false;
    return inet_addr(ip.c_str()) != INADDR_NONE;
}

bool is_self_peer_endpoint(const std::vector<std::string>& local_ips,
                           uint16_t local_port,
                           const std::string& ip,
                           uint16_t port) {
    if (port != local_port) return false;
    return std::find(local_ips.begin(), local_ips.end(), ip) != local_ips.end();
}

PeerManager::PeerManager(asio::io_context& io, Scheduler& sched, IOWorkerPool* io_pool,
                         const Sha1Digest& info_hash, uint32_t local_speed_mbps, uint16_t p2p_port)
    : io_(io), sched_(sched), io_pool_(io_pool), local_speed_mbps_(local_speed_mbps),
      acceptor_(io), p2p_port_(p2p_port) {
    asio::ip::tcp::endpoint endpoint(asio::ip::tcp::v4(), p2p_port);
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();
    memcpy(info_hash_.data(), info_hash.data(), 20);
    local_peer_ips_ = collect_local_peer_ips();
}

PeerManager::~PeerManager() {
    for (auto& s : sessions_) {
        s->set_on_handshake_done(nullptr);
        s->set_on_pex_peer(nullptr);
        s->set_on_pex_remove(nullptr);
        s->set_on_request_timeout(nullptr);
        s->set_on_disconnect(nullptr);
    }
    for (auto& s : sessions_) {
        s->disconnect();
    }
    sessions_.clear();
}

void PeerManager::start_accept() {
    do_accept();
}

void PeerManager::do_accept() {
    auto& mgr = *this;
    acceptor_.async_accept([&mgr](asio::error_code ec, asio::ip::tcp::socket sock) {
        if (!ec) {
            std::string remote_ip = "?";
            uint16_t remote_port = 0;
            {
                asio::error_code ign;
                auto ep = sock.remote_endpoint(ign);
                if (!ign) {
                    remote_ip = ep.address().to_string();
                    remote_port = ep.port();
                }
            }
            std::cerr << "[do_accept] new inbound from " << remote_ip << ":" << remote_port
                      << " existing=" << mgr.sessions_.size() << std::endl;
            if (mgr.sessions_.size() >= MAX_PEERS) {
                mgr.do_accept();
                return;
            }
            auto sess = std::make_shared<PeerSession>(
                mgr.io_, mgr.info_hash_, mgr.local_speed_mbps_, mgr.p2p_port_);
            sess->set_scheduler(&mgr.sched_);
            sess->set_io_pool(mgr.io_pool_);
            sess->set_file_fd(mgr.file_fd_);
            sess->set_chunk_offsets(mgr.chunk_offsets_);
            sess->set_on_handshake_done([&mgr](std::shared_ptr<PeerSession> s) {
                mgr.sched_.on_peer_speed(s->slot_id(), s->link_speed_reported());
                mgr.note_recent_connect(s);
                if (!mgr.initial_bitfield_.empty()) {
                    std::cerr << "[peer] sending bitfield, size=" << mgr.initial_bitfield_.size() << std::endl;
                    auto bf = build_bitfield(mgr.initial_bitfield_);
                    std::cerr << "[peer] bitfield built, bytes=" << bf.size() << std::endl;
                    s->send_message(std::move(bf));
                    std::cerr << "[peer] bitfield sent" << std::endl;
                }
                auto peers = make_full_pex_list(mgr.sessions_, s.get());
                if (!peers.empty()) s->send_message(build_pex(false, peers));
            });
            sess->start_inbound(std::move(sock),
                [&mgr](std::shared_ptr<PeerSession> s) { mgr.on_peer_disconnected(std::move(s)); });
            mgr.on_peer_connected(std::move(sess));
        }
        mgr.do_accept();
    });
}

void PeerManager::connect_to(const std::string& ip, uint16_t port, uint8_t flags) {
    if (sessions_.size() >= MAX_PEERS) return;
    if (!is_connectable_peer_ip(ip)) {
        std::cerr << "[connect_to] SKIP invalid peer ip " << ip << ":" << port << std::endl;
        return;
    }

    std::cerr << "[connect_to] trying " << ip << ":" << port << " flags=" << static_cast<int>(flags)
              << " existing_sessions=" << sessions_.size() << std::endl;

    if (is_self_peer_endpoint(local_peer_ips_, p2p_port_, ip, port)) {
        std::cerr << "[connect_to] SKIP self-connect to " << ip << std::endl;
        return;
    }

    for (auto& s : sessions_) {
        std::string existing_ip = s->remote_ip();
        std::cerr << "[connect_to]   check existing: ip=" << existing_ip
                  << " slot=" << s->slot_id() << std::endl;
        if (existing_ip == ip) {
            std::cerr << "[connect_to] SKIP duplicate for " << ip << std::endl;
            return;
        }
    }

    std::cerr << "[connect_to] creating new session to " << ip << std::endl;
    auto sess = std::make_shared<PeerSession>(io_, info_hash_, local_speed_mbps_, p2p_port_);
    sess->set_scheduler(&sched_);
    sess->set_io_pool(io_pool_);
    sess->set_file_fd(file_fd_);
    sess->set_chunk_offsets(chunk_offsets_);
    sess->set_on_handshake_done([this](std::shared_ptr<PeerSession> s) {
        sched_.on_peer_speed(s->slot_id(), s->link_speed_reported());
        note_recent_connect(s);
        if (!initial_bitfield_.empty()) {
            std::cerr << "[peer] sending bitfield(out), size=" << initial_bitfield_.size() << std::endl;
            auto bf = build_bitfield(initial_bitfield_);
            std::cerr << "[peer] bitfield built(out), bytes=" << bf.size() << std::endl;
            s->send_message(std::move(bf));
            std::cerr << "[peer] bitfield sent(out)" << std::endl;
        }
        auto peers = make_full_pex_list(sessions_, s.get());
        if (!peers.empty()) s->send_message(build_pex(false, peers));
    });
    sess->start_outbound(ip, port,
        [this](std::shared_ptr<PeerSession> s) { on_peer_disconnected(std::move(s)); });
    on_peer_connected(std::move(sess));
}

void PeerManager::on_peer_connected(std::shared_ptr<PeerSession> sess) {
    uint32_t id = next_slot_id_++;
    sess->set_slot_id(id);
    sess->set_on_pex_peer([this](const std::string& ip, uint16_t port, uint8_t flags) {
        if (sessions_.size() >= MAX_PEERS) return;
        if (!is_connectable_peer_ip(ip)) return;
        for (auto& s : sessions_) {
            if (s->remote_ip() == ip) return;
        }
        connect_to(ip, port, flags);
    });
    sess->set_on_pex_remove([this](const std::string& ip, uint16_t /*port*/) {
        recent_connects_.erase(ip);
    });
    sess->set_on_request_timeout([this](uint32_t chunk_idx, uint32_t begin) {
        sched_.on_subblock_timeout(chunk_idx, begin);
    });
    sessions_.push_back(sess);
    sched_.on_peer_added(id, sess->link_speed_reported());
}

void PeerManager::on_peer_disconnected(std::shared_ptr<PeerSession> sess) {
    sched_.on_peer_removed(sess->slot_id());
    note_recent_disconnect(sess);
    sessions_.erase(std::remove(sessions_.begin(), sessions_.end(), sess), sessions_.end());
}

void PeerManager::note_recent_connect(const std::shared_ptr<PeerSession>& sess) {
    if (!sess) return;
    std::string ip = sess->remote_ip();
    if (!is_connectable_peer_ip(ip)) return;

    uint8_t derived_flags = (sess->link_speed_reported() >= 1000) ? 0x02 : 0x00;
    uint16_t listen_port = sess->remote_listen_port();
    recent_connects_[ip] = {
        std::chrono::steady_clock::now(),
        listen_port != 0 ? listen_port : sess->remote_port(),
        derived_flags
    };
}

void PeerManager::note_recent_disconnect(const std::shared_ptr<PeerSession>& sess) {
    if (!sess) return;
    std::string ip = sess->remote_ip();
    if (!is_connectable_peer_ip(ip)) return;

    uint8_t derived_flags = (sess->link_speed_reported() >= 1000) ? 0x02 : 0x00;
    uint16_t listen_port = sess->remote_listen_port();
    recent_disconnects_[ip] = {
        std::chrono::steady_clock::now(),
        listen_port != 0 ? listen_port : sess->remote_port(),
        derived_flags
    };
}

std::vector<std::string> PeerManager::collect_local_peer_ips() const {
    std::vector<std::string> ips;
    ips.push_back("127.0.0.1");

    asio::error_code ec;
    auto local_ep = acceptor_.local_endpoint(ec);
    if (!ec) {
        std::string listen_ip = local_ep.address().to_string();
        if (is_connectable_peer_ip(listen_ip))
            ips.push_back(listen_ip);
    }

    std::string detected_ip = get_local_ip();
    if (is_connectable_peer_ip(detected_ip))
        ips.push_back(detected_ip);

    std::sort(ips.begin(), ips.end());
    ips.erase(std::unique(ips.begin(), ips.end()), ips.end());
    return ips;
}

PeerSession* PeerManager::get_session(uint32_t slot_id) {
    for (auto& s : sessions_) {
        if (s->slot_id() == slot_id) return s.get();
    }
    return nullptr;
}

void PeerManager::tick_choke() {
    if (sessions_.empty()) return;

    for (auto& s : sessions_) s->set_choked(true);

    for (auto& s : sessions_) {
        s->update_download_rate();
        s->update_upload_rate();
    }

    uint32_t current_upload_kbps = 0;
    for (auto& s : sessions_) current_upload_kbps += s->upload_rate_kbps();
    uint32_t current_upload_mbps = current_upload_kbps / 1000;
    uint32_t idle_speed = local_speed_mbps_ > current_upload_mbps
        ? local_speed_mbps_ - current_upload_mbps
        : 0;
    uint32_t slots = std::min(4u + idle_speed / 10 * 2, 20u);

    std::vector<std::shared_ptr<PeerSession>> sorted = sessions_;
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) { return a->download_rate_kbps() > b->download_rate_kbps(); });

    uint32_t tit_for_tat = slots * 50 / 100;
    uint32_t optimistic = slots * 25 / 100;
    uint32_t anti_starvation = slots - tit_for_tat - optimistic;

    for (uint32_t i = 0; i < sorted.size() && tit_for_tat > 0; i++) {
        if (sorted[i]->is_peer_interested()) {
            sorted[i]->set_choked(false);
            tit_for_tat--;
        }
    }

    std::mt19937 rng(std::random_device{}());
    std::shuffle(sorted.begin(), sorted.end(), rng);
    for (uint32_t i = 0; i < std::min(optimistic, static_cast<uint32_t>(sorted.size())); i++) {
        sorted[i]->set_choked(false);
    }

    for (auto& s : sorted) {
        if (anti_starvation == 0) break;
        if (s->link_speed_reported() < 1000 && s->is_choked()) {
            s->set_choked(false);
            anti_starvation--;
        }
    }

    for (auto& s : sessions_) {
        s->send_message(s->is_choked()
            ? build_message(P2PMsgId::CHOKE, nullptr, 0)
            : build_message(P2PMsgId::UNCHOKE, nullptr, 0));
    }
}

void PeerManager::tick_pex() {
    std::vector<PexPeer> delta;
    auto cutoff = std::chrono::steady_clock::now() - std::chrono::seconds(60);

    for (auto& [ip, recent] : recent_connects_) {
        if (recent.when > cutoff && is_connectable_peer_ip(ip) && recent.port != 0) {
            PexPeer p{};
            p.ip = inet_addr(ip.c_str());
            p.port = htons(recent.port);
            p.flags = recent.flags;
            delta.push_back(p);
        }
    }

    for (auto& [ip, recent] : recent_disconnects_) {
        if (recent.when > cutoff && is_connectable_peer_ip(ip) && recent.port != 0) {
            PexPeer p{};
            p.ip = inet_addr(ip.c_str());
            p.port = htons(recent.port);
            p.flags = recent.flags | 0x80;
            delta.push_back(p);
        }
    }

    if (!delta.empty()) {
        auto pex_msg = build_pex(true, delta);
        for (auto& s : sessions_) s->send_message(pex_msg);
    }

    recent_connects_.clear();
    recent_disconnects_.clear();
}

} // namespace thinbt
