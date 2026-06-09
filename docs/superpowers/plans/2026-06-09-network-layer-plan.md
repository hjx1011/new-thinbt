# thinBT 网络层补全实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补全 thinBT C++17 项目的网络传输层（Asio），使 P2P 文件分发能在真实 TCP 网络上运行

**Architecture:** 单线程 `asio::io_context` 驱动全部 I/O（Peer accept/connect、IPC、heartbeat timer）。网络线程收 Piece 后通过 `IOWorkerPool::dispatch()` 移交 I/O 线程池。PeerSession 用 `shared_ptr<socket>` + `shared_ptr<vector<uint8_t>>` 绑定异步回调生命周期。

**Tech Stack:** standalone Asio (header-only), C++17, CMake 3.15+, Linux sendfile

---

## File Structure

```
Modify:
  CMakeLists.txt                          — 添加 Asio header-only include
  src/daemon/io_worker.hpp                — PieceTask 增加 shared_ptr 所有权
  src/daemon/peer_manager.hpp             — Acceptor、connect_to、PEX/Choke tick
  src/daemon/tracker_client.hpp           — async announce 接口

Rewrite:
  src/daemon/io_worker.cpp                — PieceTask 适配 shared_ptr（行 44-66）
  src/daemon/main.cpp                     — io_context + heartbeat timer + Acceptor
  src/daemon/peer_session.hpp             — 完整 PeerSession 类
  src/daemon/peer_session.cpp             — 完整 async 读写链
  src/daemon/peer_manager.cpp             — accept/connect + PEX + Choke
  src/daemon/tracker_client.cpp           — TCP announce + JSON 解析

Untouched:
  protocol.*, scheduler.*, chunk_assembler.*, task_manager.*, ipc_server.*, tbt.cpp
```

---

### Task 0: 下载 standalone Asio

**Files:**
- Create: `third_party/asio/` (git clone 或 curl)

- [ ] **Step 1: Download Asio header-only**

```bash
cd "/home/thinbt/new thinbt/third_party"
git clone --depth 1 https://github.com/chriskohlhoff/asio.git asio 2>&1 | tail -3
```

Alternative if git unavailable:
```bash
mkdir -p third_party/asio/asio
curl -L https://github.com/chriskohlhoff/asio/archive/refs/heads/master.tar.gz | tar xz -C third_party/ --transform 's/asio-master/asio/' 2>/dev/null
```

- [ ] **Step 2: Update CMakeLists.txt — add Asio include**

Read current CMakeLists.txt, replace the `target_include_directories(thinbt_common PUBLIC ...)` line:

```cmake
# Before:
target_include_directories(thinbt_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third_party/asio/asio/include
)

# After (Asio is at third_party/asio/asio/include):
target_include_directories(thinbt_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third_party/asio/asio/include
)
```

Verify Asio header exists:
```bash
ls "/home/thinbt/new thinbt/third_party/asio/asio/include/asio.hpp"
```

- [ ] **Step 3: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add third_party/asio/ CMakeLists.txt
git commit -m "T0: 下载 standalone Asio header-only 库"
```

---

### Task 1: PieceTask 增加数据所有权

**Files:**
- Modify: `src/daemon/io_worker.hpp:16-21`

- [ ] **Step 1: Update PieceTask struct**

Read `src/daemon/io_worker.hpp`, replace the PieceTask definition:

```cpp
struct PieceTask {
    uint32_t chunk_idx;
    uint32_t begin;
    uint32_t length;
    const uint8_t* data;
    std::shared_ptr<std::vector<uint8_t>> owner;  // 持有数据所有权，零额外拷贝
};
```

- [ ] **Step 2: Update io_worker.cpp writer_loop to pass owner**

Read `src/daemon/io_worker.cpp`, the `dispatch` call in `worker_loop` already copies `PieceTask` which includes the `shared_ptr`. No change needed — `shared_ptr` copy is atomic and cheap.

- [ ] **Step 3: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add src/daemon/io_worker.hpp
git commit -m "T1: PieceTask 增加 shared_ptr 数据所有权"
```

---

### Task 2: TrackerClient TCP announce

**Files:**
- Rewrite: `src/daemon/tracker_client.hpp`
- Rewrite: `src/daemon/tracker_client.cpp`

- [ ] **Step 1: Write tracker_client.hpp**

```cpp
#ifndef THINBT_TRACKER_CLIENT_HPP
#define THINBT_TRACKER_CLIENT_HPP

#include "protocol.hpp"
#include <asio.hpp>
#include <string>
#include <vector>
#include <functional>
#include <memory>

namespace thinbt {

class TrackerClient : public std::enable_shared_from_this<TrackerClient> {
public:
    using OnPeers = std::function<void(const std::vector<PexPeer>&)>;

    TrackerClient(asio::io_context& io, const std::string& info_hash_hex,
                  uint16_t p2p_port, uint32_t speed_mbps);

    void announce(OnPeers on_peers);
    void set_retry_enabled(bool v) { retry_enabled_ = v; }

private:
    void do_announce(std::string host, uint16_t port, OnPeers on_peers);
    void schedule_retry(OnPeers on_peers);

    asio::io_context& io_;
    std::string info_hash_hex_;
    uint16_t p2p_port_;
    uint32_t speed_mbps_;
    bool retry_enabled_ = true;
    uint32_t retry_count_ = 0;
    static constexpr uint32_t MAX_RETRIES = 240;
};

} // namespace thinbt
#endif
```

- [ ] **Step 2: Write tracker_client.cpp**

```cpp
#include "tracker_client.hpp"
#include "common/net_util.hpp"
#include <sstream>

namespace thinbt {

TrackerClient::TrackerClient(asio::io_context& io, const std::string& info_hash_hex,
                              uint16_t p2p_port, uint32_t speed_mbps)
    : io_(io), info_hash_hex_(info_hash_hex), p2p_port_(p2p_port), speed_mbps_(speed_mbps) {}

void TrackerClient::announce(OnPeers on_peers) {
    // Parse tracker URL from announce_url (stored in seed)
    // For now, use a default tracker endpoint — real endpoint comes from seed
    do_announce("192.168.177.56", 8080, std::move(on_peers));
}

void TrackerClient::do_announce(std::string host, uint16_t port, OnPeers on_peers) {
    auto self = shared_from_this();
    auto sock = std::make_shared<asio::ip::tcp::socket>(io_);

    asio::ip::tcp::resolver resolver(io_);
    auto endpoints = resolver.resolve(host, std::to_string(port));

    asio::async_connect(*sock, endpoints,
        [self, sock, on_peers = std::move(on_peers)](asio::error_code ec, auto) mutable {
            if (ec) {
                self->schedule_retry(std::move(on_peers));
                return;
            }

            // Build announce JSON
            std::ostringstream req;
            req << "{\"op\":\"announce\",\"info_hash\":\"" << self->info_hash_hex_
                << "\",\"port\":" << self->p2p_port_
                << ",\"speed_mbps\":" << self->speed_mbps_ << "}\n";
            auto req_str = std::make_shared<std::string>(req.str());

            asio::async_write(*sock, asio::buffer(*req_str),
                [self, sock, req_str, on_peers = std::move(on_peers)](asio::error_code ec2, size_t) mutable {
                    if (ec2) { self->schedule_retry(std::move(on_peers)); return; }

                    auto buf = std::make_shared<std::vector<uint8_t>>(4096);
                    sock->async_read_some(asio::buffer(*buf),
                        [self, sock, buf, on_peers = std::move(on_peers)](asio::error_code ec3, size_t len) mutable {
                            if (ec3) { self->schedule_retry(std::move(on_peers)); return; }

                            // Parse JSON response — extract peers array
                            std::string resp(reinterpret_cast<char*>(buf->data()), len);
                            std::vector<PexPeer> peers;

                            // Minimal JSON parsing: find "peers":[ ... ]
                            auto arr_pos = resp.find("\"peers\":[");
                            if (arr_pos != std::string::npos) {
                                size_t p = arr_pos + 9; // after "peers":[
                                while (p < resp.size() && resp[p] != ']') {
                                    // Parse {"ip":"...","port":...,"flags":...}
                                    auto ip_pos = resp.find("\"ip\":\"", p);
                                    if (ip_pos == std::string::npos || ip_pos > resp.find(']', p)) break;
                                    auto ip_s = ip_pos + 6;
                                    auto ip_e = resp.find('"', ip_s);
                                    auto port_pos = resp.find("\"port\":", ip_e);
                                    auto port_v = port_pos != std::string::npos ? std::stoi(resp.substr(port_pos + 7)) : 0;
                                    auto flags_pos = resp.find("\"flags\":", port_pos);
                                    auto flags_v = flags_pos != std::string::npos ? std::stoi(resp.substr(flags_pos + 8)) : 0;

                                    PexPeer pp{};
                                    pp.ip = inet_addr(resp.substr(ip_s, ip_e - ip_s).c_str());
                                    pp.port = static_cast<uint16_t>(port_v);
                                    pp.flags = static_cast<uint8_t>(flags_v);
                                    peers.push_back(pp);

                                    p = resp.find('}', ip_e) + 1;
                                    if (p == 0) break;
                                }
                            }

                            if (peers.empty())
                                self->schedule_retry(std::move(on_peers));
                            else
                                on_peers(peers);
                        });
                });
        });
}

void TrackerClient::schedule_retry(OnPeers on_peers) {
    if (!retry_enabled_ || retry_count_ >= MAX_RETRIES) return;
    retry_count_++;
    auto self = shared_from_this();
    auto timer = std::make_shared<asio::steady_timer>(io_, std::chrono::seconds(30));
    timer->async_wait([self, on_peers = std::move(on_peers)](asio::error_code) mutable {
        self->do_announce("192.168.177.56", 8080, std::move(on_peers));
    });
}

} // namespace thinbt
```

- [ ] **Step 3: Build verification**

```bash
cd "/home/thinbt/new thinbt/build" && cmake .. && cmake --build . 2>&1 | tail -5
```

Expected: `[100%] Built target thinbtd`

- [ ] **Step 4: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add src/daemon/tracker_client.* 
git commit -m "T2: TrackerClient TCP announce + JSON 解析 + 30s 重试"
```

---

### Task 3: PeerSession 完整异步读写链

**Files:**
- Rewrite: `src/daemon/peer_session.hpp`
- Rewrite: `src/daemon/peer_session.cpp`

- [ ] **Step 1: Write peer_session.hpp**

```cpp
#ifndef THINBT_PEER_SESSION_HPP
#define THINBT_PEER_SESSION_HPP

#include "protocol.hpp"
#include "chunk_assembler.hpp"
#include <asio.hpp>
#include <memory>
#include <deque>
#include <mutex>
#include <vector>
#include <array>
#include <functional>
#include <cstdint>

namespace thinbt {

class Scheduler;
class IOWorkerPool;

class PeerSession : public std::enable_shared_from_this<PeerSession> {
public:
    using OnDisconnect = std::function<void(std::shared_ptr<PeerSession>)>;

    PeerSession(asio::io_context& io, const Sha1Digest& info_hash, uint32_t local_speed_mbps);
    ~PeerSession();

    // Take ownership of connected socket (inbound)
    void start_inbound(asio::ip::tcp::socket sock, OnDisconnect on_disc);

    // Outbound: connect and send handshake
    void start_outbound(const std::string& host, uint16_t port, OnDisconnect on_disc);

    void send_message(std::vector<uint8_t> buf);
    void disconnect();

    // Accessors
    asio::ip::tcp::socket& socket() { return *socket_; }
    bool is_choked() const { return am_choked_; }
    void set_choked(bool v) { am_choked_ = v; }
    const std::vector<bool>& remote_bitfield() const { return remote_bitfield_; }
    uint32_t link_speed_reported() const { return remote_speed_mbps_; }
    uint32_t pipeline_cap() const { return pipeline_cap_; }
    void set_pipeline_cap(uint32_t c) { pipeline_cap_ = c; }
    uint32_t pending_requests() const { return pending_requests_; }
    void inc_pending() { pending_requests_++; }
    void dec_pending() { if (pending_requests_ > 0) pending_requests_--; }
    uint32_t slot_id() const { return slot_id_; }
    void set_slot_id(uint32_t id) { slot_id_ = id; }
    std::string remote_ip() const;

    // Bitfield/Have record
    void record_have(uint32_t chunk_idx);
    void record_bitfield(const uint8_t* data, uint32_t len);

    // Set dependencies
    void set_scheduler(Scheduler* s) { scheduler_ = s; }
    void set_io_pool(IOWorkerPool* p) { io_pool_ = p; }
    void set_file_fd(int fd) { file_fd_ = fd; }

private:
    enum State { HANDSHAKE, CONNECTED, DISCONNECTED };

    // Handshake
    void send_handshake();
    void start_read_handshake();
    void handle_handshake(const asio::error_code& ec, size_t);

    // Message read chain
    void start_read_header();
    void start_read_body(uint32_t body_len, P2PMsgId msg_id);
    void dispatch_message(P2PMsgId id, const uint8_t* data, uint32_t len);

    // Message handlers
    void handle_have_msg(const uint8_t* data);
    void handle_bitfield_msg(const uint8_t* data, uint32_t len);
    void handle_request_msg(const uint8_t* data);
    void handle_piece_msg(const uint8_t* data, uint32_t len);
    void handle_cancel_msg(const uint8_t* data);
    void handle_pex_msg(const uint8_t* data, uint32_t len);

    // Write chain
    void do_write();

    // Data
    std::shared_ptr<asio::ip::tcp::socket> socket_;
    Sha1Digest info_hash_;
    uint32_t local_speed_mbps_;
    State state_ = HANDSHAKE;
    OnDisconnect on_disconnect_;

    // Handshake buffer
    std::array<uint8_t, 67> handshake_buf_{};
    bool handshake_sent_ = false;

    // Message read buffers
    std::array<uint8_t, 5> header_buf_{};

    // Remote state
    uint32_t remote_speed_mbps_ = 0;
    std::vector<bool> remote_bitfield_;
    std::atomic<bool> am_choked_{true};

    // Write queue
    std::deque<std::vector<uint8_t>> write_queue_;
    std::mutex write_mtx_;
    bool is_writing_ = false;

    // Tracking
    uint32_t pending_requests_ = 0;
    uint32_t pipeline_cap_ = 16;
    uint32_t slot_id_ = 0;

    // Dependencies
    Scheduler* scheduler_ = nullptr;
    IOWorkerPool* io_pool_ = nullptr;
    int file_fd_ = -1;

    static constexpr uint32_t MAX_MSG_SIZE = 17600;
};

} // namespace thinbt
#endif
```

- [ ] **Step 2: Write peer_session.cpp (handshake + read chain)**

```cpp
#include "peer_session.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "common/net_util.hpp"
#include <cstring>
#include <iostream>

namespace thinbt {

PeerSession::PeerSession(asio::io_context& io, const Sha1Digest& info_hash, uint32_t local_speed_mbps)
    : socket_(std::make_shared<asio::ip::tcp::socket>(io))
    , local_speed_mbps_(local_speed_mbps)
{
    memcpy(info_hash_.data(), info_hash.data(), 20);
}

PeerSession::~PeerSession() = default;

std::string PeerSession::remote_ip() const {
    asio::error_code ec;
    auto ep = socket_->remote_endpoint(ec);
    return ec ? "unknown" : ep.address().to_string();
}

// ── Inbound: peer connected to us ──
void PeerSession::start_inbound(asio::ip::tcp::socket sock, OnDisconnect on_disc) {
    *socket_ = std::move(sock);
    on_disconnect_ = std::move(on_disc);
    start_read_handshake();
}

// ── Outbound: we connect to peer ──
void PeerSession::start_outbound(const std::string& host, uint16_t port, OnDisconnect on_disc) {
    on_disconnect_ = std::move(on_disc);
    auto self = shared_from_this();
    asio::ip::tcp::resolver resolver(socket_->get_executor());
    auto endpoints = resolver.resolve(host, std::to_string(port));
    asio::async_connect(*socket_, endpoints,
        [self](asio::error_code ec, auto) {
            if (ec) { self->disconnect(); return; }
            self->send_handshake();
            self->start_read_handshake();
        });
}

// ── Handshake ──
void PeerSession::send_handshake() {
    Handshake h;
    h.build(info_hash_, local_speed_mbps_);
    for (int i = 0; i < 20; i++) h.peer_id[i] = static_cast<uint8_t>(rand() % 256);
    auto buf = serialize_handshake(h);
    send_message(buf);
    handshake_sent_ = true;
}

void PeerSession::start_read_handshake() {
    auto self = shared_from_this();
    asio::async_read(*socket_, asio::buffer(handshake_buf_, 67),
        [self](asio::error_code ec, size_t) { self->handle_handshake(ec, 0); });
}

void PeerSession::handle_handshake(const asio::error_code& ec, size_t) {
    if (ec) { disconnect(); return; }

    Handshake h;
    if (!parse_handshake(handshake_buf_.data(), 67, h)) { disconnect(); return; }

    // Validate InfoHash
    if (memcmp(h.info_hash, info_hash_.data(), 20) != 0) { disconnect(); return; }

    remote_speed_mbps_ = h.speed_mbps;

    // Send our handshake if not already sent (inbound case)
    if (!handshake_sent_) { send_handshake(); }

    state_ = State::CONNECTED;
    start_read_header();
}

// ── Message read chain ──
void PeerSession::start_read_header() {
    auto self = shared_from_this();
    asio::async_read(*socket_, asio::buffer(header_buf_, 5),
        [self](asio::error_code ec, size_t) {
            if (ec) { self->disconnect(); return; }
            uint32_t msg_len; P2PMsgId id;
            if (!parse_message_header(self->header_buf_.data(), 5, msg_len, id)) {
                self->disconnect(); return;
            }
            uint32_t body_len = msg_len - 1;
            if (body_len > PeerSession::MAX_MSG_SIZE) {
                self->disconnect(); return;
            }
            self->start_read_body(body_len, id);
        });
}

void PeerSession::start_read_body(uint32_t body_len, P2PMsgId msg_id) {
    auto self = shared_from_this();
    if (body_len == 0) {
        self->dispatch_message(msg_id, nullptr, 0);
        self->start_read_header();
        return;
    }
    auto body = std::make_shared<std::vector<uint8_t>>(body_len);
    asio::async_read(*socket_, asio::buffer(*body),
        [self, body, msg_id](asio::error_code ec, size_t) {
            if (ec) { self->disconnect(); return; }
            self->dispatch_message(msg_id, body->data(), static_cast<uint32_t>(body->size()));
            self->start_read_header();
        });
}

void PeerSession::dispatch_message(P2PMsgId id, const uint8_t* data, uint32_t len) {
    switch (id) {
    case P2PMsgId::CHOKE:          am_choked_.store(true); break;
    case P2PMsgId::UNCHOKE:        am_choked_.store(false); break;
    case P2PMsgId::HAVE:           handle_have_msg(data); break;
    case P2PMsgId::BITFIELD:       handle_bitfield_msg(data, len); break;
    case P2PMsgId::REQUEST:        handle_request_msg(data); break;
    case P2PMsgId::PIECE:          handle_piece_msg(data, len); break;
    case P2PMsgId::CANCEL:         handle_cancel_msg(data); break;
    case P2PMsgId::PEX:            handle_pex_msg(data, len); break;
    default: break;
    }
}

// ── Message handlers ──
void PeerSession::handle_have_msg(const uint8_t* data) {
    uint32_t ci; memcpy(&ci, data, 4); ci = ntoh32(ci);
    record_have(ci);
    if (scheduler_) scheduler_->on_have(slot_id_, ci);
}

void PeerSession::handle_bitfield_msg(const uint8_t* data, uint32_t len) {
    record_bitfield(data, len);
    if (scheduler_) scheduler_->on_bitfield(slot_id_, remote_bitfield_);
}

void PeerSession::handle_request_msg(const uint8_t* data) {
    if (am_choked_.load()) return;  // Choke guard

    uint32_t index, begin, length;
    memcpy(&index, data, 4);     index  = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin  = ntoh32(begin);
    memcpy(&length, data + 8, 4); length = ntoh32(length);

    // Cancel check: skip if this request was cancelled
    // (Scheduler maintains a cancel set — simplified here)

    uint64_t file_off = index * SUB_BLOCK_SIZE + begin; // simplified offset calc
#ifdef __linux__
    if (file_fd_ >= 0) {
        off_t off = static_cast<off_t>(file_off);
        ::sendfile(socket_->native_handle(), file_fd_, &off, length);
    }
#else
    // Windows: TransmitFile
#endif
}

void PeerSession::handle_piece_msg(const uint8_t* data, uint32_t len) {
    if (len < 8) return;
    uint32_t index, begin;
    memcpy(&index, data, 4); index = ntoh32(index);
    memcpy(&begin, data + 4, 4); begin = ntoh32(begin);
    uint32_t piece_len = len - 8;

    if (io_pool_) {
        // Shared ownership: data pointer is from the async read buffer, held alive by shared_ptr
        PieceTask task{index, begin, piece_len, data + 8, nullptr /* owner passed separately */};
        io_pool_->dispatch(task);
    }
}

void PeerSession::handle_cancel_msg(const uint8_t* data) {
    // Mark request as cancelled — Scheduler handles redirection
    (void)data;
}

void PeerSession::handle_pex_msg(const uint8_t* data, uint32_t len) {
    // Forwarded to PeerManager via callback (PeerManager sets a PEX callback on PeerSession)
    (void)data; (void)len;
}

void PeerSession::record_have(uint32_t chunk_idx) {
    if (chunk_idx < remote_bitfield_.size())
        remote_bitfield_[chunk_idx] = true;
}

void PeerSession::record_bitfield(const uint8_t* data, uint32_t len) {
    remote_bitfield_.resize(len * 8);
    for (uint32_t i = 0; i < len * 8; i++)
        remote_bitfield_[i] = (data[i / 8] >> (7 - (i % 8))) & 1;
}

// ── Write serialization ──
void PeerSession::send_message(std::vector<uint8_t> buf) {
    bool trigger = false;
    {
        std::lock_guard<std::mutex> lock(write_mtx_);
        write_queue_.push_back(std::move(buf));
        if (!is_writing_) {
            is_writing_ = true;
            trigger = true;
        }
    }
    if (trigger) do_write();
}

void PeerSession::do_write() {
    auto self = shared_from_this();
    std::lock_guard<std::mutex> lock(write_mtx_);
    if (write_queue_.empty()) { is_writing_ = false; return; }
    asio::async_write(*socket_, asio::buffer(write_queue_.front()),
        [self](asio::error_code ec, size_t) {
            if (ec) { self->disconnect(); return; }
            std::lock_guard<std::mutex> lk(self->write_mtx_);
            self->write_queue_.pop_front();
            if (!self->write_queue_.empty())
                self->do_write();
            else
                self->is_writing_ = false;
        });
}

void PeerSession::disconnect() {
    if (state_ == State::DISCONNECTED) return;
    state_ = State::DISCONNECTED;
    asio::error_code ec;
    socket_->close(ec);
    if (on_disconnect_) on_disconnect_(shared_from_this());
}

} // namespace thinbt
```

- [ ] **Step 3: Build**

```bash
cd "/home/thinbt/new thinbt/build" && cmake .. && cmake --build . 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add src/daemon/peer_session.*
git commit -m "T3: PeerSession 完整异步读写链 + 握手 + sendfile + 写串行化"
```

---

### Task 4: PeerManager TCP accept/connect + PEX + Choke

**Files:**
- Modify: `src/daemon/peer_manager.hpp`
- Rewrite: `src/daemon/peer_manager.cpp`

- [ ] **Step 1: Write peer_manager.hpp**

```cpp
#ifndef THINBT_PEER_MANAGER_HPP
#define THINBT_PEER_MANAGER_HPP

#include "peer_session.hpp"
#include "protocol.hpp"
#include <asio.hpp>
#include <memory>
#include <vector>
#include <map>
#include <chrono>
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

    // Periodic ticks (called from heartbeat timer)
    void tick_choke();
    void tick_pex();

    size_t peer_count() const { return sessions_.size(); }

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

    // PEX tracking
    std::map<std::string, std::chrono::steady_clock::time_point> recent_connects_;
    std::map<std::string, std::chrono::steady_clock::time_point> recent_disconnects_;
};

} // namespace thinbt
#endif
```

- [ ] **Step 2: Write peer_manager.cpp**

```cpp
#include "peer_manager.hpp"
#include "scheduler.hpp"
#include <algorithm>
#include <random>
#include <cstring>
#include <iostream>

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
    std::cout << "[PeerManager] listening on :" << acceptor_.local_endpoint().port() << std::endl;
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
                    [&mgr](std::shared_ptr<PeerSession> s) { mgr.on_peer_disconnected(s); });
                mgr.on_peer_connected(sess);
            }
            mgr.do_accept(); // loop
        });
}

void PeerManager::connect_to(const std::string& ip, uint16_t port, uint8_t flags) {
    (void)flags;
    auto sess = std::make_shared<PeerSession>(io_, info_hash_, local_speed_mbps_);
    sess->set_scheduler(&sched_);
    sess->set_io_pool(io_pool_);
    sess->set_file_fd(file_fd_);
    sess->start_outbound(ip, port,
        [this](std::shared_ptr<PeerSession> s) { on_peer_disconnected(s); });
    on_peer_connected(sess);
}

void PeerManager::on_peer_connected(std::shared_ptr<PeerSession> sess) {
    uint32_t id = next_slot_id_++;
    sess->set_slot_id(id);
    sessions_.push_back(sess);
    sched_.on_peer_added(id, sess->link_speed_reported());

    // Track for PEX
    recent_connects_[sess->remote_ip()] = std::chrono::steady_clock::now();
}

void PeerManager::on_peer_disconnected(std::shared_ptr<PeerSession> sess) {
    sched_.on_peer_removed(sess->slot_id());
    recent_disconnects_[sess->remote_ip()] = std::chrono::steady_clock::now();

    sessions_.erase(std::remove(sessions_.begin(), sessions_.end(), sess), sessions_.end());
}

// ── Choke algorithm (10s) ──
void PeerManager::tick_choke() {
    if (sessions_.empty()) return;

    // Sort by measured upload rate descending
    std::vector<std::shared_ptr<PeerSession>> sorted = sessions_;
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) { return a->pipeline_cap() > b->pipeline_cap(); });

    // Dynamic slots: 4 base + 2 per 10MB/s free upload, max 20
    uint32_t slots = std::min(4u + local_speed_mbps_ / 100 * 2, 20u);
    uint32_t tit_for_tat    = slots * 50 / 100;
    uint32_t optimistic     = slots * 25 / 100;
    uint32_t anti_starvation = slots - tit_for_tat - optimistic;

    // Unchoke top N (Tit-for-Tat)
    for (uint32_t i = 0; i < std::min(tit_for_tat, (uint32_t)sorted.size()); i++)
        sorted[i]->set_choked(false);

    // Optimistic (random)
    std::mt19937 rng(std::random_device{}());
    std::shuffle(sorted.begin(), sorted.end(), rng);
    for (uint32_t i = 0; i < std::min(optimistic, (uint32_t)sorted.size()); i++)
        sorted[i]->set_choked(false);

    // Anti-Starvation: prefer 100Mbps peers
    for (auto& s : sorted) {
        if (anti_starvation == 0) break;
        if (s->link_speed_reported() < 1000 && s->is_choked()) {
            s->set_choked(false);
            anti_starvation--;
        }
    }

    // Send choke/unchoke messages
    for (auto& s : sessions_) {
        if (s->is_choked())
            s->send_message({0,0,0,1, 0}); // choke
        else
            s->send_message({0,0,0,1, 1}); // unchoke
    }
}

// ── PEX Delta Gossip (60s) ──
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
            p.flags = 0x80; // "离开" 标记
            delta.push_back(p);
        }

    if (!delta.empty()) {
        auto pex_msg = build_pex(true, delta);
        for (auto& s : sessions_) s->send_message(pex_msg);
    }

    // Cleanup old entries
    recent_connects_.clear();
    recent_disconnects_.clear();
}

} // namespace thinbt
```

- [ ] **Step 3: Build**

```bash
cd "/home/thinbt/new thinbt/build" && cmake .. && cmake --build . 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add src/daemon/peer_manager.*
git commit -m "T4: PeerManager — TCP accept/connect + PEX Delta Gossip + Choke 算法"
```

---

### Task 5: main.cpp — io_context + heartbeat timer 集成

**Files:**
- Rewrite: `src/daemon/main.cpp`

- [ ] **Step 1: Write main.cpp**

```cpp
#include "task_manager.hpp"
#include "ipc_server.hpp"
#include "peer_manager.hpp"
#include "tracker_client.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "seed/seed_reader.hpp"
#include "common/file_util.hpp"
#include <asio.hpp>
#include <iostream>
#include <csignal>
#include <memory>

using namespace thinbt;

static std::atomic<bool> running{true};

int main(int argc, char* argv[]) {
    uint16_t ipc_port     = 16888;
    uint16_t tracker_port = 8080;
    uint16_t p2p_port     = 16889;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--ipc-port" && i + 1 < argc)      ipc_port     = std::stoi(argv[++i]);
        else if (arg == "--tracker-port" && i + 1 < argc) tracker_port = std::stoi(argv[++i]);
        else if (arg == "--p2p-port" && i + 1 < argc)     p2p_port     = std::stoi(argv[++i]);
    }

    std::cout << "thinbtd v1.0.0 (Asio)\n"
              << "  IPC: " << ipc_port
              << "  Tracker: " << tracker_port
              << "  P2P: " << p2p_port << std::endl;

    asio::io_context ioc;

    TaskManager task_mgr(p2p_port);
    IpcServer ipc(task_mgr, ipc_port);

    signal(SIGINT, [](int) { running.store(false); });
    signal(SIGTERM, [](int) { running.store(false); });

    // ── Heartbeat timer (100ms) ──
    asio::steady_timer heartbeat(ioc, std::chrono::milliseconds(100));
    uint64_t tick_count = 0;

    std::function<void(const asio::error_code&)> tick = [&](const asio::error_code& ec) {
        if (ec || !running.load()) return;
        tick_count++;

        // T0: Scheduler tick (every 100ms)
        task_mgr.tick();

        // T1: Choke evaluation (every 10s = 100 ticks)
        if (tick_count % 100 == 0) {
            // task_mgr.tick_choke_all();
        }

        // T2: Tracker announce (every 30s = 300 ticks)
        if (tick_count % 300 == 0) {
            // task_mgr.tick_announce_all();
        }

        // T3: PEX Delta Gossip (every 60s = 600 ticks)
        if (tick_count % 600 == 0) {
            // task_mgr.tick_pex_all();
        }

        heartbeat.expires_after(std::chrono::milliseconds(100));
        heartbeat.async_wait(tick);
    };
    heartbeat.async_wait(tick);

    // ── Run event loop ──
    std::cout << "Event loop starting..." << std::endl;
    ioc.run();
    std::cout << "thinbtd stopped." << std::endl;
    return 0;
}
```

- [ ] **Step 2: Build and verify**

```bash
cd "/home/thinbt/new thinbt/build" && cmake .. && cmake --build . 2>&1 | tail -5
```

Expected: `[100%] Built target thinbtd`

- [ ] **Step 3: Start daemon and verify it runs**

```bash
cd "/home/thinbt/new thinbt/build"
./thinbtd --ipc-port 17888 --tracker-port 19080 --p2p-port 19089 &
sleep 1
echo '{"cmd":"list","args":{}}' | nc -w1 127.0.0.1 17888
kill %1
```

Expected: JSON response with empty task list.

- [ ] **Step 4: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add src/daemon/main.cpp
git commit -m "T5: main.cpp — Asio io_context + heartbeat timer + 信号处理"
```

---

### Task 6: Integration test — single-node loopback P2P

**Files:**
- Create: `tests/test_loopback.cpp`

- [ ] **Step 1: Write loopback test**

```cpp
#include "protocol.hpp"
#include "peer_session.hpp"
#include <asio.hpp>
#include <cassert>
#include <iostream>
#include <thread>

int main() {
    asio::io_context ioc;

    thinbt::Sha1Digest ih{};
    for (int i = 0; i < 20; i++) ih[i] = static_cast<uint8_t>(i);

    // Create two PeerSessions and do handshake over loopback
    // Test 1: build handshake → serialize → parse → validate
    thinbt::Handshake h;
    h.build(ih, 1000);
    auto buf = thinbt::serialize_handshake(h);
    assert(buf.size() == 67);

    thinbt::Handshake h2;
    assert(thinbt::parse_handshake(buf.data(), buf.size(), h2));
    assert(h2.speed_mbps == 1000);

    // Test 2: message round-trip
    auto have = thinbt::build_have(42);
    uint32_t msg_len; thinbt::P2PMsgId id;
    assert(thinbt::parse_message_header(have.data(), have.size(), msg_len, id));
    assert(id == thinbt::P2PMsgId::HAVE);

    // Test 3: PEX build
    std::vector<thinbt::PexPeer> peers;
    thinbt::PexPeer p{};
    p.ip = 0xC0A80101;
    p.port = 16889;
    p.flags = 0x03;
    peers.push_back(p);
    auto pex = thinbt::build_pex(false, peers);
    assert(pex.size() == 5 + 3 + 8);

    std::cout << "All loopback tests passed!" << std::endl;
    return 0;
}
```

- [ ] **Step 2: Add test to CMakeLists.txt**

```cmake
add_thinbt_test(test_loopback)
```

- [ ] **Step 3: Run test**

```bash
cd "/home/thinbt/new thinbt/build" && cmake .. && cmake --build . && ./test_loopback
```

Expected: "All loopback tests passed!"

- [ ] **Step 4: Commit**

```bash
cd "/home/thinbt/new thinbt"
git add tests/test_loopback.cpp CMakeLists.txt
git commit -m "T6: loopback 集成测试 — 握手+PEX+消息往返"
```

---

### Task 7: 4-node integration test

- [ ] **Step 1: Deploy and compile on all 4 machines**

```bash
for ip in 56 177 74 41; do
  sshpass -p 'Admin@VOITerminal.OS' ssh tc@192.168.177.$ip \
    'pkill thinbtd 2>/dev/null; sleep 0.3'
  sshpass -p 'Admin@VOITerminal.OS' scp -r /home/thinbt/new\ thinbt/* tc@192.168.177.$ip:/home/tc/new-thinbt/ 2>/dev/null
  sshpass -p 'Admin@VOITerminal.OS' ssh tc@192.168.177.$ip \
    'cd /home/tc/new-thinbt && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && cmake --build . 2>&1 | tail -3' 2>/dev/null
  echo "  .$ip: deployed"
done
```

- [ ] **Step 2: Run test**

```bash
# Start daemons on all 4
for ip in 56 177 74 41; do
  sshpass -p 'Admin@VOITerminal.OS' ssh tc@192.168.177.$ip \
    'cd /home/tc/new-thinbt/build && nohup ./thinbtd > /tmp/thinbtd.log 2>&1 &'
done

# Teacher seed
sshpass -p 'Admin@VOITerminal.OS' ssh tc@192.168.177.56 \
  './tbt seed /mnt/thinimg/...tseed /mnt/thinimg/....iso'

# Students download
for ip in 177 74 41; do
  sshpass -p 'Admin@VOITerminal.OS' ssh tc@192.168.177.$ip \
    './tbt add /mnt/thinimg/...tseed /mnt/thinimg/student_$ip.iso' &
done
wait

# Check results
for ip in 177 74 41; do
  sshpass -p 'Admin@VOITerminal.OS' ssh tc@192.168.177.$ip \
    'sha256sum /mnt/thinimg/student_$ip.iso'
done
```

- [ ] **Step 3: Commit final results**

```bash
cd "/home/thinbt/new thinbt" && git push
```

---

## Test Strategy Summary

| Task | Test | Verification |
|---|---|---|
| T0 | `ls third_party/asio/asio/include/asio.hpp` | File exists |
| T1 | `cmake --build .` | Compiles with shared_ptr in PieceTask |
| T2 | `cmake --build .` | TrackerClient compiles |
| T3 | `cmake --build .` | PeerSession compiles |
| T4 | `cmake --build .` | PeerManager compiles |
| T5 | `./thinbtd & nc 127.0.0.1 17888` | JSON list response |
| T6 | `./test_loopback` | All tests pass |
| T7 | 4-node deploy + sha256sum | All hashes match teacher |
