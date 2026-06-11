#include "peer_manager.hpp"
#include "scheduler.hpp"
#include <algorithm>
#include <random>
#include <cstring>
#include <iostream>

namespace thinbt {
namespace {

std::vector<PexPeer> make_full_pex_list(
    const std::vector<std::shared_ptr<PeerSession>>& sessions,
    const PeerSession* exclude,
    uint16_t p2p_port)
{
    std::vector<PexPeer> peers;
    for (const auto& sess : sessions) {
        if (!sess || sess.get() == exclude) continue;
        std::string ip = sess->remote_ip();
        if (ip.empty() || ip == "unknown") continue;

        PexPeer p{};
        p.ip = inet_addr(ip.c_str());
        p.port = htons(p2p_port);
        p.flags = sess->link_speed_reported() >= 1000 ? 0x02 : 0x00;
        peers.push_back(p);
    }
    return peers;
}

} // namespace

PeerManager::PeerManager(asio::io_context& io, Scheduler& sched, IOWorkerPool* io_pool,
                          const Sha1Digest& info_hash, uint32_t local_speed_mbps, uint16_t p2p_port)
    : io_(io), sched_(sched), io_pool_(io_pool), local_speed_mbps_(local_speed_mbps),
      acceptor_(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), p2p_port)), p2p_port_(p2p_port)
{
    memcpy(info_hash_.data(), info_hash.data(), 20);
}

PeerManager::~PeerManager() {
    // 先清除所有 session 的回调，防止 disconnect 时回调已销毁的 PeerManager
    for (auto& s : sessions_) {
        s->set_on_handshake_done(nullptr);
        s->set_on_pex_peer(nullptr);
        s->set_on_pex_remove(nullptr);
        s->set_on_request_timeout(nullptr);
        s->set_on_disconnect(nullptr);
    }
    // 再断开所有 session
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
    acceptor_.async_accept(
        [&mgr](asio::error_code ec, asio::ip::tcp::socket sock) {
            if (!ec) {
                std::string remote_ip = "?";
                uint16_t remote_port = 0;
                {
                    asio::error_code ign;
                    auto ep = sock.remote_endpoint(ign);
                    if (!ign) { remote_ip = ep.address().to_string(); remote_port = ep.port(); }
                }
                std::cerr << "[do_accept] new inbound from " << remote_ip << ":" << remote_port
                          << " existing=" << mgr.sessions_.size() << std::endl;
                if (mgr.sessions_.size() >= MAX_PEERS) {
                    mgr.do_accept();
                    return;
                }
                auto sess = std::make_shared<PeerSession>(mgr.io_, mgr.info_hash_, mgr.local_speed_mbps_);
                sess->set_scheduler(&mgr.sched_);
                sess->set_io_pool(mgr.io_pool_);
                sess->set_file_fd(mgr.file_fd_);
                sess->set_chunk_offsets(mgr.chunk_offsets_);
                sess->set_on_handshake_done([&mgr](std::shared_ptr<PeerSession> s) {
                    if (!mgr.initial_bitfield_.empty()) {
                        std::cerr << "[peer] sending bitfield, size=" << mgr.initial_bitfield_.size() << std::endl;
                        auto bf = build_bitfield(mgr.initial_bitfield_);
                        std::cerr << "[peer] bitfield built, bytes=" << bf.size() << std::endl;
                        s->send_message(std::move(bf));
                        std::cerr << "[peer] bitfield sent" << std::endl;
                    }
                    auto peers = make_full_pex_list(mgr.sessions_, s.get(), mgr.p2p_port_);
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

    std::cerr << "[connect_to] trying " << ip << ":" << port << " flags=" << (int)flags
              << " existing_sessions=" << sessions_.size() << std::endl;

    // 防止连接自己（Tracker 返回的 peer 列表中包含本机）
    {
        asio::error_code ec;
        auto local_ep = acceptor_.local_endpoint(ec);
        if (!ec && ip == local_ep.address().to_string() && port == p2p_port_) {
            std::cerr << "[connect_to] SKIP self-connect to " << ip << std::endl;
            return;
        }
    }

    // 去重：同一 IP 只保留一个 TCP 连接（全双工无需双向各一条）
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
    auto sess = std::make_shared<PeerSession>(io_, info_hash_, local_speed_mbps_);
    sess->set_scheduler(&sched_);
    sess->set_io_pool(io_pool_);
    sess->set_file_fd(file_fd_);
    sess->set_chunk_offsets(chunk_offsets_);
    sess->set_on_handshake_done([this](std::shared_ptr<PeerSession> s) {
        if (!initial_bitfield_.empty()) {
            std::cerr << "[peer] sending bitfield(out), size=" << initial_bitfield_.size() << std::endl;
            auto bf = build_bitfield(initial_bitfield_);
            std::cerr << "[peer] bitfield built(out), bytes=" << bf.size() << std::endl;
            s->send_message(std::move(bf));
            std::cerr << "[peer] bitfield sent(out)" << std::endl;
        }
        auto peers = make_full_pex_list(sessions_, s.get(), p2p_port_);
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
        // PEX 全量/增量新 Peer: 上限检查 + 去重后连接
        if (sessions_.size() >= MAX_PEERS) return;
        for (auto& s : sessions_) {
            if (s->remote_ip() == ip) return;
        }
        connect_to(ip, port, flags);
    });
    sess->set_on_pex_remove([this](const std::string& ip, uint16_t /*port*/) {
        // PEX Delta: 对端离开，从本地连接记录池移除
        recent_connects_.erase(ip);
    });
    // Fast Fail: 超时后通知 Scheduler 重新调度（精确到子块）
    sess->set_on_request_timeout([this](uint32_t chunk_idx, uint32_t begin) {
        sched_.on_subblock_timeout(chunk_idx, begin);
    });
    sessions_.push_back(sess);
    sched_.on_peer_added(id, sess->link_speed_reported());
    // 从握手速度推导 flags：千兆=0x02，种子节点在 PEX 中由调用方设置
    uint8_t derived_flags = (sess->link_speed_reported() >= 1000) ? 0x02 : 0x00;
    recent_connects_[sess->remote_ip()] = {std::chrono::steady_clock::now(), derived_flags};
}

void PeerManager::on_peer_disconnected(std::shared_ptr<PeerSession> sess) {
    sched_.on_peer_removed(sess->slot_id());
    // Preserve flags for PEX delta: derive from session reported speed
    uint8_t derived_flags = (sess->link_speed_reported() >= 1000) ? 0x02 : 0x00;
    recent_disconnects_[sess->remote_ip()] = {std::chrono::steady_clock::now(), derived_flags};
    sessions_.erase(std::remove(sessions_.begin(), sessions_.end(), sess), sessions_.end());
}

PeerSession* PeerManager::get_session(uint32_t slot_id) {
    for (auto& s : sessions_) {
        if (s->slot_id() == slot_id) return s.get();
    }
    return nullptr;
}

// ── Choke (10s) ──
void PeerManager::tick_choke() {
    if (sessions_.empty()) return;

    for (auto& s : sessions_) s->set_choked(true);

    // 更新所有 peer 的下载速率和上传速率（基于 10 秒窗口内收发字节数）
    for (auto& s : sessions_) {
        s->update_download_rate();
        s->update_upload_rate();
    }

    // 计算真实的闲置上行带宽
    uint32_t current_upload_kbps = 0;
    for (auto& s : sessions_)
        current_upload_kbps += s->upload_rate_kbps();
    uint32_t current_upload_mbps = current_upload_kbps / 1000;
    uint32_t idle_speed = local_speed_mbps_ > current_upload_mbps
                         ? local_speed_mbps_ - current_upload_mbps : 0;
    uint32_t slots = std::min(4u + idle_speed / 10 * 2, 20u);

    std::vector<std::shared_ptr<PeerSession>> sorted = sessions_;
    // Tit-for-Tat: 按实际下载速率排序，而非 pipeline_cap（预估上限）
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) { return a->download_rate_kbps() > b->download_rate_kbps(); });

    uint32_t tit_for_tat    = slots * 50 / 100;
    uint32_t optimistic     = slots * 25 / 100;
    uint32_t anti_starvation = slots - tit_for_tat - optimistic;

    for (uint32_t i = 0; i < sorted.size() && tit_for_tat > 0; i++) {
        if (sorted[i]->is_peer_interested()) {
            sorted[i]->set_choked(false);
            tit_for_tat--;
        }
    }

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

    // 使用 build_message 发送 choke/unchoke + 通知 Scheduler
    for (auto& s : sessions_) {
        sched_.on_choke_change(s->slot_id(), s->is_choked());
        s->send_message(s->is_choked()
            ? build_message(P2PMsgId::CHOKE, nullptr, 0)
            : build_message(P2PMsgId::UNCHOKE, nullptr, 0));
    }
}

// ── PEX Delta (60s) ──
void PeerManager::tick_pex() {
    std::vector<PexPeer> delta;
    auto cutoff = std::chrono::steady_clock::now() - std::chrono::seconds(60);

    for (auto& [ip, pair] : recent_connects_) {
        auto& t = pair.first;
        auto flags = pair.second;
        if (t > cutoff) {
            PexPeer p{};
            p.ip   = inet_addr(ip.c_str());
            p.port = htons(16889);
            p.flags = flags;
            delta.push_back(p);
        }
    }

    for (auto& [ip, tf] : recent_disconnects_) {
        auto& t = tf.first;
        auto flags = tf.second;
        if (t > cutoff) {
            PexPeer p{};
            p.ip   = inet_addr(ip.c_str());
            p.port = htons(16889);
            p.flags = flags | 0x80; // preserve original flags, set "left" bit
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
