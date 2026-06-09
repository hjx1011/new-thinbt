# 窗口 6 提示词 — CLI + IPC + Tracker 打通

---

你在开发 thinBT C++17 项目，工作目录 `/home/thinbt/new thinbt`。

## 项目背景

项目是一个 P2P 文件分发系统。窗口 1-5 已经把网络传输层、文件 I/O、调度层、集成测试全部做完。现在还差三层打通，让用户能**真正通过命令行创建种子、启动做种、发起下载**。

## 任务概览

| 子任务 | 文件 | 改什么 |
|--------|------|--------|
| 6a | src/cli/tbt.cpp | `make` 命令真实调用 fastcdc + seed_writer |
| 6b | src/daemon/ipc_server.cpp | `seed`/`remove` 路由调用 TaskManager |
| 6c | src/daemon/tracker_server.cpp | 增加 asio TCP acceptor，监听 announce |
| 6d | src/daemon/main.cpp | 集成 tracker TCP listener 到事件循环 |
| 6e | src/cli/tbt.cpp | 补全 `update`/`peers` 命令 |

---

## 子任务 6a：CLI `make` 命令

### 当前状态（假的）

```cpp
if (cmd == "make") {
    std::cout << R"({"status":"ok","msg":"Seed file generated"})" << std::endl;
}
```

### 目标行为

```
tbt make <file_path> [-o seed_path]
```

1. 用 fastcdc 扫描文件，生成 chunk 列表（见 `src/cdc/fastcdc.hpp` 的 `fastcdc_scan_file`）
2. 计算每个 chunk 的 xxHash64
3. 生成 .tseed 文件（见 `src/seed/seed_writer.hpp`，`TSeedFile` 结构在 `src/seed/tseed.hpp`）
4. 输出 `{"status":"ok","data":{"seed_path":"...","info_hash":"...","chunk_count":N}}`

### 实现要点

- fastcdc_scan_file 返回 `std::vector<ChunkEntry>`，包含每个 chunk 的 offset/size/hash
- seed_writer 需要 `write_tseed(path, chunks, file_size, file_name, announce_url)` 类似的接口——检查 `src/seed/seed_writer.cpp` 中已有的函数签名
- 如果 seed_writer 的接口不满足需求，需要扩展它
- 默认 .tseed 输出路径为 `<file_path>.tseed`
- -o 参数可覆盖输出路径

### 验证

```bash
cd build && make -j
./tbt make /path/to/test_file.iso
# 检查生成的 .tseed 文件存在，chunk_count > 0
```

---

## 子任务 6b：IPC 路由修复

### 当前状态（假的）

[ipc_server.cpp:21](src/daemon/ipc_server.cpp#L21)：
```cpp
if (cmd == "seed") {
    response = R"({"status":"ok","data":{"task_id":"00000001"}})";  // 硬编码！
```

[ipc_server.cpp:48-49](src/daemon/ipc_server.cpp#L48-L49)：
```cpp
} else if (cmd == "remove") {
    response = R"({"status":"ok"})";  // 硬编码，没调 task_mgr_
```

### 目标行为

- `seed` → 解析 `seed_path` 和 `file_path`，调用 `task_mgr_.cmd_seed(seed_path, file_path)`
- `remove` → 解析 `task_id`，调用 `task_mgr_.cmd_remove(task_id, force)`

### 实现要点

- 仿照现有 `add` 命令的 JSON 解析逻辑（[ipc_server.cpp:24-31](src/daemon/ipc_server.cpp#L24-L31)）
- `TaskManager` 的 `cmd_seed`/`cmd_add`/`cmd_remove` 已经存在，只需要正确解析参数并调用

### 验证

```bash
# 启动 thinbtd
./thinbtd &
# 做种
./tbt seed /tmp/test.tseed /tmp/test.iso
# 查看列表
./tbt list
# 删除任务
./tbt remove <task_id>
```

---

## 子任务 6c：Tracker TCP Listener

### 当前状态

TrackerServer 只是一个内存数据结构（`std::map + mutex`），没有网络监听。Peer 无法通过网络 announce。

### 目标

给 TrackerServer 增加一个 TCP acceptor，在指定端口（默认 8080）监听 announce 请求。

### 协议格式（JSON 换行分隔）

请求：
```json
{"op":"announce","info_hash":"abc123...","port":16889,"speed_mbps":1000}
```

响应：
```json
{"op":"announce_ok","peers":[{"ip":"192.168.1.1","port":16889,"flags":2}]}
```

### 实现要点

- 给 `TrackerServer` 增加 `start_accept(asio::io_context& io)` 方法
- 或新建一个 `TrackerAcceptor` 辅助类（如果不想改 TrackerServer 的职责边界）
- 每个连接接收一行 JSON，解析 `info_hash`/`port`/`speed_mbps`，提取 `peer_ip`（从 socket remote_endpoint 获取）
- 调用 `announce(info_hash, peer_ip, port, speed_mbps)`，得到 peer 列表
- 构造 JSON 响应发回，关闭连接
- 协议和 `TrackerClient::do_announce` 中构造的请求格式一致（参考 [tracker_client.cpp:30-32](src/daemon/tracker_client.cpp#L30-L32)）

### 设计选择

推荐新建 `src/daemon/tracker_acceptor.hpp` 和 `tracker_acceptor.cpp`，不修改现有的 `TrackerServer` 类（保持数据结构纯净）。

```cpp
class TrackerAcceptor {
    TrackerAcceptor(asio::io_context& io, TrackerServer& server, uint16_t port);
    void start();
    // async accept → read line → parse JSON → server.announce() → write response → close
};
```

### 验证

```bash
# 启动 thinbtd
./thinbtd --tracker-port 8080 &
# 用 netcat 模拟 announce
echo '{"op":"announce","info_hash":"abc","port":12345,"speed_mbps":1000}' | nc 127.0.0.1 8080
# 应该收到包含 peer 列表的 JSON 响应
```

---

## 子任务 6d：main.cpp 集成 Tracker TCP

### 改动

在 main.cpp 中集成子任务 6c 的 TrackerAcceptor：

1. 启动时创建 `TrackerAcceptor`（或给 `TrackerServer` 加 accept 能力）
2. tick 中增加 `tracker.cleanup_stale()`（每 60 tick = 6s 清理一次过期 peer）
3. 确保 tracker_port 参数生效

### 验证

编译通过，thinbtd 启动后端口 8080 可连接。

---

## 子任务 6e：CLI `update`/`peers` 命令

### `tbt update <task_id>`

查询指定任务的状态：
```
tbt update <task_id>
→ {"status":"ok","data":{"progress":45.2,"speed_mib_s":12.3,"peers":3}}
```

实现：发送 `{"cmd":"update","args":{"task_id":"<tid>"}}` 到 IPC，thinbtd 返回该任务的实时状态。

需要在 ipc_server.cpp 中增加 `update` 路由，调用 `task_mgr_` 获取单个任务信息。

### `tbt peers <task_id>`

列出指定任务的已连接 peer：
```
tbt peers <task_id>
→ {"status":"ok","data":{"peers":[{"ip":"...","port":...,"speed":...,"choked":true},{"ip":"..."}]}}
```

实现：需要在 PeerManager 中增加 `get_peer_info()` 接口，IPC 路由调用它。

---

## 窗口 6 整体验证

```bash
# 1. 编译
cd build && cmake .. && make -j$(nproc)

# 2. 创建种子
dd if=/dev/urandom of=/tmp/test.iso bs=1M count=10
./tbt make /tmp/test.iso -o /tmp/test.tseed

# 3. 启动 daemon
./thinbtd --tracker-port 8080 --p2p-port 16889 --ipc-port 16888 &
sleep 1

# 4. 做种
./tbt seed /tmp/test.tseed /tmp/test.iso

# 5. 查看状态
./tbt list

# 6. 验证 tracker 端口
echo '{"op":"announce","info_hash":"test","port":12345,"speed_mbps":1000}' | nc 127.0.0.1 8080

# 7. 清理
./tbt remove <task_id>
```

全部命令返回正确的 JSON 响应，无 crash。

---

## 编码规范

- C++17，命名空间 thinbt
- 参考现有代码风格（[peer_manager.cpp](src/daemon/peer_manager.cpp)、[tracker_client.cpp](src/daemon/tracker_client.cpp)）
- 头文件保护用 `THINBT_<MODULE>_H`
- 错误处理：JSON 解析失败返回 `{"status":"error","error":"..."}`
- 新增 .cpp 记得加到 CMakeLists.txt 的对应 target
