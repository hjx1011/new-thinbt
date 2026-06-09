# thinBT 网络层补全设计

**日期**：2026-06-09  
**状态**：设计确认，待实现

---

## 1. 背景与范围

thinBT C++17 项目中，I/O 层（mmap/ChunkAssembler/IOWorkerPool）、协议层（protocol 序列化）、调度层（Scheduler）均已实现。但网络传输层仅有 stub，无法进行实际的 P2P 数据传输。

本文档定义网络层的完整实现方案，使 thinBT 能够在局域网中实际运行 P2P 文件分发。

---

## 2. 事件循环与线程模型

### 2.1 架构

```
asio::io_context (主线程，单线程)
  │
  ├─ TcpAcceptor (16889)         → 接受入站 Peer 连接
  ├─ TcpConnector                → 向 Tracker 返回的 Peer 发起出站连接
  ├─ PeerSession × N             → 每个连接一条异步读写链
  ├─ IpcAcceptor (16888)         → CLI 的 TCP/JSON 接口
  ├─ heartbeat_timer (100ms)     → 统一心跳
  │    ├─ tick % 1   → Scheduler::tick()
  │    ├─ tick % 100 → Choke 评估 (10s)
  │    ├─ tick % 300 → Tracker announce (30s)
  │    └─ tick % 600 → PEX Delta (60s)
  └─ completions 处理           → Scheduler::process_completions()
```

### 2.2 线程职责

| 线程 | 职责 | 安全机制 |
|---|---|---|
| 主线程 (io_context) | 事件循环、TCP I/O、Scheduler、协议解析 | 单线程，无竞争 |
| I/O 线程池 | memcpy→mmap + atomic 标记 | SPSC 队列 + cv |
| 计算线程池 | SHA-256 校验 | MPMC 队列 |

### 2.3 数据面与控制面解耦

- 主线程收 Piece → 立即 `dispatch()` 给 I/O 线程池 → 主线程回到读头
- `PieceTask` 携带 `shared_ptr<vector<uint8_t>>` 所有权，零额外拷贝
- Chunk 完成 → I/O 线程 push `ChunkCompleteMsg` → 主线程 `process_completions()` 处理

### 2.4 关键约束

- **Buffer 生命周期**：所有 async 操作的 buffer 必须用 `shared_ptr<vector<uint8_t>>` 绑定到 lambda 捕获
- **单个 heartbeat_timer**：不分频创建多个 timer，100ms tick 中按计数器分频
- **sendfile 零拷贝**：REQUEST 响应直接调用 `::sendfile()`（Linux）/ `::TransmitFile()`（Windows）

---

## 3. PeerSession 异步状态机

### 3.1 状态流转

```
HANDSHAKE_SENT → READ_HANDSHAKE → CONNECTED → (消息循环) → DISCONNECTED
```

### 3.2 异步读链

```cpp
void PeerSession::start_read_header() {
    auto self = shared_from_this();
    asio::async_read(socket_, asio::buffer(header_buf_, 5),
        [self](const error_code& ec, size_t) {
            if (ec) { self->disconnect(); return; }
            uint32_t msg_len; P2PMsgId id;
            parse_message_header(self->header_buf_.data(), 5, msg_len, id);
            self->start_read_body(msg_len - 1, id);
        });
}

void PeerSession::start_read_body(uint32_t body_len, P2PMsgId msg_id) {
    auto self = shared_from_this();
    auto body = std::make_shared<std::vector<uint8_t>>(body_len);
    asio::async_read(socket_, asio::buffer(*body),
        [self, body, msg_id](const error_code& ec, size_t) {
            if (ec) { self->disconnect(); return; }
            self->dispatch_message(msg_id, body->data(), body->size());
            self->start_read_header();
        });
}
```

### 3.3 消息路由

| msg_id | Handler | 行为 |
|---|---|---|
| CHOKE | `handle_choke` | 设置 `am_choked_ = true` |
| UNCHOKE | `handle_unchoke` | 设置 `am_choked_ = false`，触发 Scheduler 可能选此 Peer |
| INTERESTED | `handle_interested` | 记录对方感兴趣 |
| NOT_INTERESTED | `handle_not_interested` | 清除兴趣标记 |
| HAVE | `handle_have` | `Scheduler::on_have(slot, chunk_idx)` |
| BITFIELD | `handle_bitfield` | `Scheduler::on_bitfield(slot, bf)` |
| REQUEST | `handle_request` | 零拷贝 sendfile → 发送 Piece |
| PIECE | `handle_piece` | `io_worker_pool_->dispatch()` |
| CANCEL | `handle_cancel` | 从 pending 队列移除 |
| PEX | `handle_pex` | 转发给 PeerManager 处理 |

### 3.4 零拷贝发送

```cpp
void PeerSession::handle_request(uint32_t chunk_idx, uint32_t begin, uint32_t len) {
    if (am_choked_) return;  // Choke 状态下拒绝
    if (is_cancelled(chunk_idx, begin, len)) return;  // Endgame Cancel 检查

    uint64_t file_off = chunk_base_offset(chunk_idx) + begin;
#ifdef __linux__
    ::sendfile(socket_.native_handle(), file_fd_, (off_t*)&file_off, len);
#else
    ::TransmitFile(socket_.native_handle(), file_handle_, len, 0, NULL, NULL, TF_USE_KERNEL_APC);
#endif
}
```

### 3.5 写串行化

```cpp
class PeerSession {
    std::deque<std::vector<uint8_t>> write_queue_;
    bool is_writing_ = false;

    void send_message(std::vector<uint8_t> buf) {
        bool trigger = false;
        {
            std::lock_guard lock(write_mtx_);  // 可能被其他 Peer 的 tick 调用
            write_queue_.push_back(std::move(buf));
            if (!is_writing_) {
                is_writing_ = true;
                trigger = true;
            }
        }
        if (trigger) do_write();
    }

    void do_write() {
        auto self = shared_from_this();
        asio::async_write(socket_, asio::buffer(write_queue_.front()),
            [self](const error_code& ec, size_t) {
                std::lock_guard lock(self->write_mtx_);
                self->write_queue_.pop_front();
                if (!self->write_queue_.empty()) {
                    self->do_write();
                } else {
                    self->is_writing_ = false;
                }
            });
    }
};
```

### 3.6 安全边界

- `MAX_MSG_SIZE = 17600`（16KB Piece + 头部 + 余量），超限立即断开
- Choke 状态下收到 REQUEST → 丢弃，不触发磁盘读取
- CANCEL 检查必须在 sendfile 之前

---

## 4. Tracker Client

```cpp
class TrackerClient {
    void announce() {
        auto sock = std::make_shared<tcp::socket>(io_);
        async_connect(*sock, tracker_ep_, [this, sock](error_code ec) {
            if (ec) { retry_announce(); return; }
            std::string req = build_announce_json(info_hash_, p2p_port_, speed_);
            async_write(*sock, asio::buffer(req), [this, sock](error_code, size_t) {
                auto buf = std::make_shared<std::vector<uint8_t>>(4096);
                async_read(*sock, asio::buffer(*buf), [this, buf, sock](error_code, size_t len) {
                    auto peers = parse_announce_response(buf->data(), len);
                    on_peers_(peers);  // 回调 PeerManager 发起出站连接
                });
            });
        });
    }

    // 每 30s 重新 announce（heartbeat tick_count % 300 == 0）
    // Tracker 不可达 → 降级为 PEX-only 模式
};
```

---

## 5. PeerManager

### 5.1 连接管理

```cpp
class PeerManager {
    asio::io_context& io_;
    tcp::acceptor acceptor_;
    std::vector<std::shared_ptr<PeerSession>> sessions_;

    // 入站连接
    void start_accept();

    // 出站连接（从 Tracker 或 PEX 获取的 peer 列表）
    void connect_to(const std::string& ip, uint16_t port, uint8_t flags);

    // 握手完成回调
    void on_peer_connected(std::shared_ptr<PeerSession> sess);
};
```

### 5.2 PEX Delta Gossip (每 60s)

```
收集:
  recent_connects_:  {ip → connect_time} 最近 60s 新连接
  recent_disconnects_: {ip → disconnect_time} 最近 60s 断开

构建:
  PexPeer{ip, port, flags, reserved}
  新建节点 flags 正常值
  断开节点 flags 最高位置 1 标记"离开"

向所有已连接 Peer 广播 build_pex(is_delta=true, peers)
```

### 5.3 Choke 算法 (每 10s)

```
收集所有 Peer 的上传速率
按速率降序排列
动态槽位: slots = min(4 + 空闲上行Mbps/10*2, 20)

分配:
  50% → Tit-for-Tat（速率最高优先 unchoke）
  25% → Optimistic Unchoke（随机轮换）
  25% → Anti-Starvation（百兆节点优先）

对被 choke 的发送 choke 消息
对被 unchoke 的发送 unchoke 消息
```

---

## 6. 改动范围

| 文件 | 改动 | 估计行数 |
|---|---|---|
| `CMakeLists.txt` | 修改 | 添加 `find_package(asio)` 或 FetchContent |
| `src/daemon/main.cpp` | **重写** | `io_context` + heartbeat timer + Acceptor |
| `src/daemon/peer_session.hpp` | **重写** | 写队列、状态枚举、sendfile 接口 |
| `src/daemon/peer_session.cpp` | **重写** | 完整 async 读写链 + 握手 + 零拷贝 |
| `src/daemon/peer_manager.hpp` | 修改 | Acceptor、connect_to、Choke/PEX tick |
| `src/daemon/peer_manager.cpp` | **重写** | TCP accept/connect + PEX Delta + Choke |
| `src/daemon/tracker_client.hpp` | 修改 | async announce 接口 |
| `src/daemon/tracker_client.cpp` | **重写** | TCP announce + JSON 解析 |
| `src/daemon/io_worker.hpp` | 修改 | `PieceTask` 增加 `shared_ptr<vector<uint8_t>>` |

不动的文件：`protocol.*`, `scheduler.*`, `chunk_assembler.*`, `task_manager.*`, `ipc_server.*`, `tbt.cpp`

---

## 7. 测试策略

1. **单元测试**：单机 loopback — 启动两个 thinbtd 实例，一个做种一个下载，验证握手→Bitfield→Request→Piece 链路
2. **集成测试**：4 节点局域网 — 与旧 thinbt 相同的测试流程，比较吞吐量和 chunk 分布
3. **异常测试**：Peer 断开重连、Tracker 宕机降级 PEX、Choke 状态下拒绝 Request
