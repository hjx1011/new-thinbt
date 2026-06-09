# thinBT loopback 测试通过 + 网络层补全设计

**日期**：2026-06-09
**状态**：设计中，待确认
**依赖**：[network-layer-design.md](2026-06-09-network-layer-design.md)

---

## 1. 背景

thinBT C++17 项目的网络层代码已全部存在（PeerSession、PeerManager、TrackerClient、Scheduler 等），但 `test_loopback` 端到端测试失败（`FAIL: Handshake incomplete`），说明存在多个接线错误和逻辑遗漏。

本文档定位 4 个关键 gap 及修复方案，目标：**test_loopback 通过 → 两台进程能完成 P2P 文件传输**。

---

## 2. Gap 总览

| # | Gap | 严重度 | 影响的模块 |
|---|---|---|---|
| A | 握手后不发送 Bitfield | 🔴 致命 | peer_manager |
| B | Choke 不重置 Peer 状态 | 🔴 致命 | peer_manager |
| C | Interested/Not-Interested 空处理 | 🟡 功能缺失 | peer_session, protocol |
| D | Tracker URL 硬编码 | 🟡 可用性 | main, task_manager |
| E | Scheduler 回调是空函数 | 🔴 致命 | task_manager, main |

修复范围：全部在 `src/daemon/`，不触及 `protocol.*`、`scheduler.*`、`chunk_assembler.*`、`segment_io.*`、`ipc_server.*`。

---

## 3. Gap A：握手后发送 Bitfield

### 3.1 根因

`PeerManager::on_peer_connected()`（[peer_manager.cpp:50](src/daemon/peer_manager.cpp#L50)）只做记账（slot_id、sessions_、recent_connects_），从不向新 Peer 发送本地 Bitfield。

`set_initial_bitfield()` 存了一个成员变量 `initial_bitfield_`，但全代码库没有任何地方读取它。

### 3.2 修复方案

新增 `OnHandshakeDone` 回调：PeerSession 握手完成后通知 PeerManager，PeerManager 发送 Bitfield。

**peer_session.hpp** — 新增字段和回调：

```cpp
// 新增回调类型（在 OnDisconnect 旁）
using OnHandshakeDone = std::function<void(std::shared_ptr<PeerSession>)>;
void set_on_handshake_done(OnHandshakeDone cb) { on_handshake_done_ = std::move(cb); }

// 新增私有字段
OnHandshakeDone on_handshake_done_;
```

**peer_session.cpp** — `handle_handshake()` 末尾触发回调：

```cpp
void PeerSession::handle_handshake(const asio::error_code& ec, size_t) {
    // ... 现有验证逻辑不变 ...
    state_ = State::CONNECTED;
    start_read_header();

    // 新增：通知 PeerManager 握手完成
    if (on_handshake_done_) on_handshake_done_(shared_from_this());
}
```

**peer_manager.cpp** — do_accept() 和 connect_to() 中设置回调：

```cpp
// 在创建 PeerSession 后、start_xxx 之前设置
sess->set_on_handshake_done([this](std::shared_ptr<PeerSession> s) {
    if (!initial_bitfield_.empty()) {
        s->send_message(build_bitfield(initial_bitfield_));
    }
});
```

### 3.3 验证

`test_loopback` 应能从 `connected=0` 变为 `connected=1`，且 peer 端 `remote_bitfield().size() > 0`。

---

## 4. Gap B：Choke 状态重置

### 4.1 根因

`PeerManager::tick_choke()`（[peer_manager.cpp:68](src/daemon/peer_manager.cpp#L68)）只做 unchoke（`set_choked(false)`），从不重置。上一轮被 unchoke 的 Peer 永久保持 unchoke 状态，Choke 算法完全失效。

### 4.2 修复方案

在 `tick_choke()` 开头先重置所有 Peer 为 choked：

```cpp
void PeerManager::tick_choke() {
    if (sessions_.empty()) return;

    // 新增：先重置全部为 choked
    for (auto& s : sessions_) s->set_choked(true);

    // ... 后续排序和选择性 unchoke 不变 ...
}
```

### 4.3 风险

无。这是 Choke 算法的标准行为。

---

## 5. Gap C：Interested/Not-Interested 完整实现

### 5.1 当前状态

- `P2PMsgId::INTERESTED` 和 `P2PMsgId::NOT_INTERESTED` 已在枚举中定义
- `dispatch_message()` 中 handler 是空操作（只注释不做事）
- 没有 `build_interested()` / `build_not_interested()` 工厂函数
- PeerSession 没有 `peer_interested_` 状态追踪

### 5.2 修复方案

#### 5.2.1 protocol.hpp / protocol.cpp — 新增工厂函数

```cpp
// protocol.hpp 新增声明
std::vector<uint8_t> build_interested();
std::vector<uint8_t> build_not_interested();
```

```cpp
// protocol.cpp 新增实现
std::vector<uint8_t> build_interested() {
    return build_message(P2PMsgId::INTERESTED, nullptr, 0);
}

std::vector<uint8_t> build_not_interested() {
    return build_message(P2PMsgId::NOT_INTERESTED, nullptr, 0);
}
```

INTERESTED/NOT_INTERESTED 消息体为 5 字节（4 字节 len=1 + 1 字节 id），无 payload。

#### 5.2.2 peer_session.hpp — 新增状态

```cpp
// 新增原子标志
std::atomic<bool> peer_interested_{false};

// 新增公开方法
bool is_peer_interested() const { return peer_interested_.load(std::memory_order_acquire); }
void send_interested();
void send_not_interested();
bool am_interested() const { return am_interested_.load(std::memory_order_acquire); }

// 新增私有字段
std::atomic<bool> am_interested_{false};
```

#### 5.2.3 peer_session.cpp — 实现 handler

```cpp
// dispatch_message() 中替换空 handler：
case P2PMsgId::INTERESTED:
    peer_interested_.store(true, std::memory_order_release);
    break;
case P2PMsgId::NOT_INTERESTED:
    peer_interested_.store(false, std::memory_order_release);
    break;
```

```cpp
// UNCHOKE handler 增强：收到 unchoke 后表达兴趣
case P2PMsgId::UNCHOKE:
    am_choked_.store(false, std::memory_order_release);
    // 如果我们还有缺失 chunk，发送 INTERESTED
    if (scheduler_ && scheduler_->missing_count() > 0)
        send_interested();
    break;
```

```cpp
// 新增方法
void PeerSession::send_interested() {
    am_interested_.store(true, std::memory_order_release);
    send_message(build_interested());
}

void PeerSession::send_not_interested() {
    am_interested_.store(false, std::memory_order_release);
    send_message(build_not_interested());
}
```

#### 5.2.4 peer_manager.cpp — Choke 只用感兴趣的 Peer

```cpp
// tick_choke() 中排序前过滤/排序：
// 只 unchoke 那些对我们感兴趣的 peer
for (uint32_t i = 0; i < std::min(tit_for_tat, (uint32_t)sorted.size()); i++) {
    if (!sorted[i]->is_peer_interested()) continue;
    sorted[i]->set_choked(false);
}
```

### 5.3 消息语义

| 消息 | 发送时机 | 含义 |
|---|---|---|
| INTERESTED (我→Peer) | 收到 UNCHOKE 且我还有缺失 chunk | 我需要你的数据 |
| NOT_INTERESTED (我→Peer) | 本地下载完成 | 我不再需要数据 |
| INTERESTED (Peer→我) | Peer 收到我的 UNCHOKE | Peer 需要我的数据 |
| NOT_INTERESTED (Peer→我) | Peer 下载完成 | Peer 不再需要数据 |

### 5.4 验证

Choke tick 后，只有 `is_peer_interested() == true` 的 Peer 被 unchoke。test_loopback 中 seed 应在收到 Bitfield 后发送 INTERESTED（如果它也有缺失 chunk — seed 不会，但流程应正确）。

---

## 6. Gap D：Tracker URL 可配置

### 6.1 当前状态

- `main.cpp` 已有 `--tracker-port` 参数
- `task_manager.cpp` 回退硬编码 `192.168.177.56:8080`
- seed 文件的 `announce_url` 解析逻辑已存在于 `tick_tracker_announce()`（143-155 行），但回退值是硬编码的

### 6.2 修复方案

#### 6.2.1 main.cpp — 新增参数

```cpp
// 新增局部变量
std::string tracker_host = "";   // 空 = 从 seed 解析 / 默认值
uint16_t    tracker_port = 8080;

// 解析参数
if (arg == "--tracker-host" && i + 1 < argc) tracker_host = argv[++i];
```

#### 6.2.2 TaskManager 构造函数 — 接受 tracker 配置

```cpp
// task_manager.hpp
TaskManager(uint16_t p2p_port, 
            const std::string& tracker_host = "",
            uint16_t tracker_port = 8080);

// 新增成员
std::string tracker_host_;
uint16_t tracker_port_;
```

#### 6.2.3 task_manager.cpp — 优先级逻辑

```cpp
void TaskManager::tick_tracker_announce(asio::io_context& io) {
    for (auto& [tid, t] : tasks_) {
        // 优先级: CLI 参数 > seed announce_url > hardcoded fallback
        std::string host = tracker_host_;
        uint16_t port = tracker_port_;

        if (host.empty() && t->seed) {
            // 从 seed URL 解析
            auto proto_pos = t->seed->announce_url.find("thinbt://");
            if (proto_pos != std::string::npos) {
                // ... 现有解析逻辑 ...
            }
        }
        if (host.empty()) host = "127.0.0.1";  // 安全的默认回退

        // ... 后续 announce 不变 ...
    }
}
```

### 6.3 验证

`./thinbtd --tracker-host 192.168.1.100 --tracker-port 9090` 应使用指定地址，而非硬编码 IP。

---

## 7. Gap E：Scheduler 回调接线（main.cpp + TaskManager 集成）

### 7.1 当前状态

**main.cpp 的问题：**
- 第 55-59 行创建了一个独立 `global_sched`，与任何 Task 都不关联（死代码）
- TaskManager 内部每个 ActiveTask 有自己独立的 Scheduler

**task_manager.cpp 的问题：**
- `cmd_seed()` 和 `cmd_add()` 创建 Scheduler 时，`issue_request_` 和 `broadcast_have_` 回调是空 lambda
- 这意味着 `Scheduler::tick()` 永远不会发出 REQUEST 消息
- PeerManager 在 `tick_tracker_announce()` 中延迟创建（第 163-169 行）

**根因链路：**
```
Scheduler::tick() → issue_request_(peer, ci, begin, len) → 空函数 → 无 REQUEST 发出
                     ↑
                     这个回调需要找到 PeerSession 并 send_message(build_request(...))
                     ↑
                     但 PeerManager 还没创建 / Scheduler 不知道 PeerManager 的存在
```

### 7.2 修复方案

#### 7.2.1 main.cpp — 删除死代码

删除第 55-59 行的 `global_sched`。main.cpp 中所有调度逻辑应通过 `task_mgr.tick()` 驱动。

```cpp
// 删除这段：
// auto global_sched = std::make_unique<Scheduler>();
// global_sched->init(0, 1000, ...);
```

#### 7.2.2 TaskManager — 存储 io_context 引用，提前创建 PeerManager

```cpp
// task_manager.hpp 构造函数签名变更
class TaskManager {
public:
    TaskManager(asio::io_context& io, uint16_t p2p_port,
                const std::string& tracker_host = "",
                uint16_t tracker_port = 8080);
private:
    asio::io_context& io_;   // 新增
    // ... tracker_host_, tracker_port_ 新增 ...
};
```

#### 7.2.3 task_manager.cpp — cmd_seed/cmd_add 中提前创建 PeerManager

在 `cmd_seed()` 和 `cmd_add()` 末尾（tasks_[tid] = ... 之前），立即创建 PeerManager：

```cpp
// 在 cmd_seed() 中，scheduler 创建之后：
task->peer_mgr = std::make_unique<PeerManager>(
    io_, *task->scheduler, task->io_pool.get(),
    task->seed->info_hash, 1000, p2p_port_);

// 如果本地是 seed（有完整文件），设置 bitfield
std::vector<bool> full_bf(chunk_count, true);
task->peer_mgr->set_initial_bitfield(full_bf);
task->peer_mgr->start_accept();

// 接线 scheduler 回调
auto* pm = task->peer_mgr.get();
task->scheduler->init(chunk_count, 1000,
    // issue_request_: 找到 PeerSession 发送 REQUEST
    [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
        auto* sess = pm->get_session(slot_id);
        if (sess) {
            sess->send_message(build_request(chunk_idx, begin, length));
            sess->inc_pending();
        }
    },
    // broadcast_have_: 向所有已连接 Peer 广播 HAVE
    [pm](uint32_t chunk_idx) {
        auto have_msg = build_have(chunk_idx);
        for (auto& s : pm->sessions()) s->send_message(have_msg);
    });
```

#### 7.2.4 task_manager.cpp — tick_tracker_announce 去掉延迟创建

第 163-169 行，去掉 `if (!t->peer_mgr) { ... }` 的延迟创建逻辑（PeerManager 已在 cmd_seed/cmd_add 中创建）。

保留 PeerManager 尚未创建时的容错处理（防御性编程）。

#### 7.2.5 main.cpp — 更新构造函数调用

```cpp
// 传入 io_context 引用
TaskManager task_mgr(ioc, p2p_port, tracker_host, tracker_port);
```

### 7.3 数据流验证（修复后）

```
heartbeat (100ms)
  → task_mgr.tick()
    → scheduler->tick()
      → 选出 rarest-first chunk + best peer
      → issue_request_(slot_id, chunk_idx, begin, length)
        → pm->get_session(slot_id)->send_message(build_request(...))  ← 不再是空函数！
          → asio::async_write → peer 收到 REQUEST
            → peer handle_request_msg → sendfile → PIECE 回来
              → handle_piece_msg → io_pool->dispatch(PieceTask)
                → assembler.on_piece() → ChunkCompleteMsg
                  → process_completions() → broadcast_have_(chunk_idx)
                    → 所有 sessions 广播 HAVE  ← 不再是空函数！
```

### 7.4 关于 test_loopback 的特殊处理

`test_loopback.cpp` 直接使用 `PeerManager` 和 `Scheduler`，不走 `TaskManager` → `cmd_seed/cmd_add`。它手动创建 PeerManager、设置 bitfield、手动发送 request。

这意味着 **test_loopback 不经过 Gap E 的代码路径**。Gap E 主要影响 `thinbtd` 守护进程通过 CLI 创建任务后的数据流。但修复 Gap E 仍然关键，因为它是「真实 daemon 运行」的前提。

---

## 8. 改动范围汇总

| 文件 | 改动 | 行数估计 |
|---|---|---|
| `src/daemon/peer_session.hpp` | 新增 OnHandshakeDone 回调、peer_interested_、am_interested_、方法声明 | +15 |
| `src/daemon/peer_session.cpp` | handle_handshake 触发回调、INTERESTED/NOT_INTERESTED/UNCHOKE handler 增强、send_interested/send_not_interested | +30 |
| `src/daemon/peer_manager.cpp` | do_accept/connect_to 加 on_handshake_done 回调、tick_choke 加重置+兴趣过滤 | +15 / -5 |
| `src/daemon/peer_manager.hpp` | 不变（已有 initial_bitfield_、get_session） | 0 |
| `src/daemon/protocol.hpp` | 新增 build_interested/build_not_interested 声明 | +2 |
| `src/daemon/protocol.cpp` | 新增 build_interested/build_not_interested 实现 | +8 |
| `src/daemon/task_manager.hpp` | 构造函数签名变更、新增 io_/tracker_host_/tracker_port_ 成员 | +5 |
| `src/daemon/task_manager.cpp` | cmd_seed/cmd_add 提前创建 PeerManager + 接线回调、tick_tracker_announce 去延迟创建、tracker URL 优先级 | +30 / -20 |
| `src/daemon/main.cpp` | 删除 global_sched、新增 --tracker-host 参数、更新 TaskManager 构造调用 | +5 / -8 |
| **总计** | | **~+120 / -40** |

不动的文件：`scheduler.*`、`chunk_assembler.*`、`io_worker.*`、`segment_io.*`、`ipc_server.*`、`tracker_server.*`、`tracker_acceptor.*`、`tracker_client.*`、`protocol.cpp` 其余函数。

---

## 9. 窗口拆分（按并行度）

### 窗口 1: Gap A + Gap B（peer_manager + peer_session handshake）

**文件：** `peer_session.hpp`, `peer_session.cpp`, `peer_manager.cpp`

**改动：**
- peer_session: 新增 `OnHandshakeDone` 回调，handle_handshake 末尾触发
- peer_manager: do_accept / connect_to 设置 on_handshake_done → 发送 initial_bitfield_
- peer_manager: tick_choke 开头重置全部 choked

**依赖：** 无
**验证：** test_loopback connected=1，remote_bitfield 非空

---

### 窗口 2: Gap C（Interested/Not-Interested）

**文件：** `protocol.hpp`, `protocol.cpp`, `peer_session.hpp`, `peer_session.cpp`, `peer_manager.cpp`

**改动：**
- protocol: build_interested() / build_not_interested()
- peer_session: peer_interested_ 原子、send_interested/send_not_interested、UNCHOKE handler 增强
- peer_manager: tick_choke 只 unchoke 感兴趣的 peer

**依赖：** 窗口 1（需要在 PeerManager 的 tick_choke 上改，窗口 1 也改这里，注意合并）
**验证：** Choke 日志/测试验证仅 interested peer 被 unchoke

---

### 窗口 3: Gap D（Tracker URL 可配置）

**文件：** `main.cpp`, `task_manager.hpp`, `task_manager.cpp`

**改动：**
- main.cpp: --tracker-host 参数
- task_manager: 构造函数新增 tracker_host/tracker_port 成员
- tick_tracker_announce: 三级优先级（CLI > seed URL > 默认值）

**依赖：** 无（完全独立于窗口 1/2）
**验证：** `./thinbtd --tracker-host X --tracker-port Y` 发起 announce 到正确地址

---

### 窗口 4: Gap E（Scheduler 回调接线 + main.cpp 清理）

**文件：** `main.cpp`, `task_manager.hpp`, `task_manager.cpp`

**改动：**
- main.cpp: 删除 global_sched，更新 TaskManager 构造调用
- task_manager: 存 io_context 引用，cmd_seed/cmd_add 中提前创建 PeerManager，接线 scheduler 回调
- task_manager: tick_tracker_announce 中去掉延迟创建 PeerManager

**依赖：** 窗口 1（PeerManager 的 send_message/bug fix 必须存在）
**验证：** thinbtd 启动后通过 CLI 创建任务，日志中能看到 REQUEST/HAVE 消息发送

---

## 10. 合并风险点

- **窗口 2 和窗口 1 都改 peer_manager.cpp**：窗口 2 的 "Choke 只 unchoke interested peer" 写在 tick_choke() 中，窗口 1 的 "重置 choked" 也写在 tick_choke() 中。需要人工合并时注意顺序：**先重置 → 再按 interested 过滤 unchoke**。

- **窗口 4 和窗口 3 都改 main.cpp 和 task_manager.hpp/cpp**：构造函数签名都变了，需要合并。

- 建议合并顺序：窗口 1 → 窗口 2 → 窗口 4 → 窗口 3。或者窗口 1 + 窗口 3 先并行，然后窗口 2 + 窗口 4 并行（各自处理自己的 merge）。

---

## 11. 最终验证

```bash
cd build && cmake .. && cmake --build . 2>&1 | tail -5
# 预期：[100%] Built target thinbtd

./test_loopback
# 预期：[PASS] End-to-end P2P transfer OK!

./test_seed && ./test_fastcdc && ./test_chunk_assembler && ./test_segment_io
# 预期：全部 PASS（回归验证）
```
