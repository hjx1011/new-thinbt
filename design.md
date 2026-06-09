# thinBT 系统设计文档

**版本**：1.0  
**语言**：C++17/20  
**日期**：2026-06-09  
**状态**：设计阶段

---

## 1. 项目背景与问题陈述

### 1.1 业务场景

- **设备规模**：1 台教师机 + 60 台学生机，千兆交换机互联
- **网络拓扑**：千兆/百兆端口混合，教师机仅有 1 个千兆上行端口
- **分发内容**：虚拟机镜像（qcow2/vmdk），单文件 20GB ~ 100GB
- **分发频率**：每期课程首次全量分发 + 课程中多次增量更新（安装软件、放置练习文件）
- **学生机磁盘**：可能是机械硬盘（HDD），随机写性能极差（~30MB/s vs 顺序写 ~150MB/s）

### 1.2 三大核心痛点

#### 痛点一：小修改导致全量重传

| | 固定分块 (BitTorrent) | 内容定义分块 (CDC) |
|---|---|---|
| 修改 50MB 后传输量 | 50GB（全量） | ~50MB（仅变更块） |
| 根因 | 插入数据导致所有块边界偏移，哈希全部失效 | 边界跟随内容移动，未变块的哈希保持不变 |

**量化影响**：千兆网络下，50GB 全量传输约 7-8 分钟。如果只需传输 50MB 变更部分，耗时仅 1-2 秒。效率差距约 **400 倍**。

#### 痛点二：千兆/百兆混合网络的短板效应

千兆节点被分配到百兆源后，TCP 流控将实际吞吐拉低到百兆级别。

- 理想：10 台千兆设备相互传输，吞吐可达 1000 Mbps × 10 = 10 Gbps
- 实际：混合调度下，有效吞吐被拉低到 100 Mbps 级别
- 总耗时可能**增加 5-10 倍**

**根因**：BitTorrent 协议的 Rarest First 和随机选择策略在设计时未考虑 Peer 的上传能力差异。Choke/Unchoke 的 Tit-for-Tat 收敛需要 10-30 秒周期，在文件分发的前几分钟，错误的选择已经造成了带宽浪费。

#### 痛点三：现有方案与场景不匹配

qBittorrent 功能冗余（DHT/PEX/磁力链接/RSS 等 80% 功能用不上），无法定制调度算法。BitTorrent 协议本身不支持 CDC 增量，且为公网设计（TCP 拥塞控制保守、Pipeline 深度浅、超时时间长），在局域网场景下吞吐无法达到物理极限。

### 1.3 设计目标

| 目标 | 衡量标准 |
|---|---|
| 增量效率 | 修改 100MB，传输量 < 150MB |
| 带宽利用 | 千兆节点下载速度 > 100 MB/s |
| 部署简单 | 单文件可执行，静态链接，无需安装依赖 |
| 磁盘友好 | HDD 顺序写，避免随机写导致的性能崩塌 |

---

## 2. 系统架构

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                    thinBT 系统架构 (C++17)                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐   TCP/JSON (127.0.0.1:16888)   ┌──────────────┐  │
│  │ tbt CLI  │◄──────────────────────────────►│   thinbtd    │  │
│  └──────────┘                                 │  (Daemon)    │  │
│                                               ├──────────────┤  │
│                                               │ IPC Server   │  │
│                                               ├──────────────┤  │
│                    ┌─────────────────────────►│ Tracker Svr  │  │
│                    │                          ├──────────────┤  │
│                    │       ┌──────────────────│ Task Manager │  │
│                    │       │                  ├──────────────┤  │
│                    │       │   ┌─────────────►│ Peer Manager │  │
│                    │       │   │              ├──────────────┤  │
│                    │       │   │   ┌──────────│ Scheduler    │  │
│                    │       │   │   │          ├──────────────┤  │
│                    │       │   │   │   ┌──────│ I/O Engine   │  │
│                    │       │   │   │   │      ├──────────────┤  │
│                    │       │   │   │   │      │ CDC Scanner  │  │
│                    │       │   │   │   │      └──────────────┘  │
│                    │       │   │   │   │                        │
│  ┌─────────────────┼───────┼───┼───┼───┼──────────────────┐    │
│  │  事件循环层     │       │   │   │   │                   │    │
│  │  epoll (Linux) / IOCP (Windows)                         │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐  │    │
│  │  │ TCP      │ │ Tracker  │ │ Peer     │ │ IPC       │  │    │
│  │  │ Acceptor │ │ Client   │ │ Conns x N│ │ Listener  │  │    │
│  │  └──────────┘ └──────────┘ └──────────┘ └───────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  计算层 (独立线程，通过无锁队列通信)                       │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │ I/O 线程池   │  │ Hash Worker  │  │ Verify Worker│   │    │
│  │  │ (4~8 线程)   │  │ (xxHash/     │  │ (SHA-256     │   │    │
│  │  │ memcpy→mmap  │  │  SHA-256)    │  │  校验)       │   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 分层架构

```
┌──────────────────────────────┐
│  CLI 层          (tbt)       │  用户交互
├──────────────────────────────┤
│  IPC 层          (TCP/JSON)  │  进程间通信
├──────────────────────────────┤
│  调度层          (Scheduler) │  Rarest First / Endgame / 速率加权
├──────────────────────────────┤
│  P2P 协议层      (Peer Mgr)  │  握手 / 消息收发 / PEX Gossip
├──────────────────────────────┤
│  I/O 层          (I/O Engine)│  mmap / sendfile / 顺序写 / 预分配
├──────────────────────────────┤
│  CDC 层          (CDC Scan)  │  FastCDC 分块 / 哈希计算
└──────────────────────────────┘

Tracker 层 — 横向服务，被 P2P 协议层和调度层引用，负责 Peer 发现与 Gossip 容灾
```

**分层规则**：上层只调用直接下层，不可跨层。Tracker 层作为横向服务，被 P2P 协议层和调度层引用。

### 2.3 模块职责

| 模块 | 类/文件 | 职责 |
|---|---|---|
| **CLI** | `tbt` | 命令行入口，构造 JSON 请求发往 thinbtd |
| **IPC Server** | `IpcServer` | TCP 监听 `127.0.0.1:16888`，JSON 解析，调用内部 API |
| **Task Manager** | `TaskManager` | 管理多个任务生命周期（seed/add/update/remove） |
| **Peer Manager** | `PeerManager` | 管理 Peer 连接池，速率探测，PEX/Gossip 消息交换 |
| **Scheduler** | `Scheduler` | Rarest First 选块、Endgame 精准补漏、带宽加权 Peer 选择 |
| **I/O Engine** | `IOEngine` | mmap 映射、sendfile 零拷贝、ChunkAssembler 无锁拼装 |
| **CDC Scanner** | `FastCDC` | 内容定义分块，SHA-256 哈希计算 |
| **Tracker Server** | `TrackerServer` | 内置 Tracker，维护 Peer 列表，心跳清理 |
| **Tracker Client** | `TrackerClient` | 向 Tracker 注册/查询，Gossip 兜底 |

### 2.4 数据流

```
发送端 (Seeder):
  文件 → FastCDC 分块 → 生成种子(.tseed) → Tracker 注册 → 等待 Peer 连接
                                                              │
接收端 (Leecher):                                             │
  获取种子 → 扫描本地文件(CDC) → 比对哈希 → 构建位图           │
     │                                                        │
     ├─→ Tracker 查询 Peer 列表 ←─────────────────────────────┘
     │
     ├─→ 连接多个 Peer，交换 Bitfield
     │
     ├─→ Scheduler 选择 Chunk + Peer（Rarest First + 速率加权）
     │
     ├─→ Peer → sendfile 零拷贝推流
     │
     ├─→ I/O 线程池 → memcpy → mmap 映射区（Page Cache）
     │
     ├─→ ChunkAssembler 无锁拼装 → SHA-256 校验
     │
     └─→ 内核 flusher 线程按 LBA 排序 → 顺序大块写入磁盘
```

---

## 3. 详细设计

### 3a. 种子文件格式 (.tseed)

#### 3a.1 二进制布局

所有字段使用 Big-Endian 网络字节序。

```
┌──────────────────────────────────────────────────┐
│ Header (固定 92 字节)                             │
├──────────────────────────────────────────────────┤
│ magic          uint32  0x54425344 ("TBSD")       │
│ version        uint16  0x0001                    │
│ flags          uint16  保留                      │
│ chunk_count    uint32  Chunk 总数                │
│ file_size      uint64  原始文件大小 (字节)        │
│ min_chunk      uint32  最小 Chunk 大小            │
│ avg_chunk      uint32  平均 Chunk 大小            │
│ max_chunk      uint32  最大 Chunk 大小            │
│ file_name_len  uint32  文件名长度 (字节)          │
│ announce_len   uint32  Tracker URL 长度           │
│ reserved       uint8[20] 保留                    │
│ file_sha256    uint8[32] 完整文件 SHA-256         │
├──────────────────────────────────────────────────┤
│ Variable Data (紧随 Header)                      │
├──────────────────────────────────────────────────┤
│ file_name      char[file_name_len]  UTF-8 文件名  │
│ announce       char[announce_len]   thinbt://... │
│ chunks[]       ChunkEntry[chunk_count]  (每个 44 字节) │
└──────────────────────────────────────────────────┘

ChunkEntry (固定 44 字节):
┌──────────────────────────────────────────────────┐
│ offset         uint64   文件中的偏移              │
│ length         uint32   Chunk 长度 (1B ~ 1MB)    │
│ sha256         uint8[32] 内容 SHA-256            │
└──────────────────────────────────────────────────┘
```

#### 3a.2 C++ 结构体

```cpp
#pragma pack(push, 1)
struct TSeedHeader {
    static constexpr uint32_t MAGIC = 0x54425344;

    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t chunk_count;
    uint64_t file_size;
    uint32_t min_chunk_size;
    uint32_t avg_chunk_size;
    uint32_t max_chunk_size;
    uint32_t file_name_len;
    uint32_t announce_len;
    uint8_t  reserved[20];
    uint8_t  file_sha256[32];
};

struct ChunkEntry {
    uint64_t offset;
    uint32_t length;
    uint8_t  sha256[32];
};
#pragma pack(pop)
```

**字节序转换规范**：文件采用 Big-Endian 存储，而 x86/x64 和 ARM 架构为 Little-Endian。读取 `.tseed` 到 `TSeedHeader` / `ChunkEntry` 后，所有大于 1 字节的整型字段（`uint16_t`, `uint32_t`, `uint64_t`）必须显式调用 `ntohs()` / `ntohl()` / `ntohll()` 转换为本机字节序后才能参与逻辑运算。写入文件时同理，调用 `htons()` / `htonl()` / `htonll()` 转换回 Big-Endian。

#### 3a.3 Tracker URL 格式

```
thinbt://<host>:<port>/announce

示例:
  thinbt://192.168.1.10:8080/announce  (IPv4)
  thinbt://[2001:db8::1]:8080/announce (IPv6)

解析方式: 标准 URL 解析 → 提取 host/port → TCP 连接
应用层协议: TCP + 单行 JSON (\n 分帧)，非 HTTP
```

#### 3a.4 InfoHash 定义

InfoHash = SHA-1(`file_sha256`(32 字节) + 所有 `ChunkEntry[]` 数组（`chunk_count × 44` 字节）)，共 20 字节。用于 Tracker announce 和 P2P 握手时标识同一 swarm。

**设计理由**：InfoHash 绝对不包含 `file_name` 和 `announce` 等可变元数据。若对整个 `.tseed` 文件做哈希：
- Tracker IP 变更 → announce 字段变化 → InfoHash 变化
- 同一个文件的旧种子学生和新种子学生因 InfoHash 不同而互相拒绝握手
- 导致 Swarm 脑裂（网络割裂），同一文件无法互通

剥离可变元数据后，只要 `file_sha256` 和 `ChunkEntry[]` 不变（即文件物理内容不变），InfoHash 永远唯一，确保 Swarm 绝不分裂。

Daemon 为每个任务额外分配一个 8 字符小写十六进制 `task_id`，用于 IPC 操作（list/remove）。

#### 3a.5 生成参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--min-size` | 16 KB | 最小 Chunk |
| `--avg-size` | 128 KB | 目标平均 Chunk |
| `--max-size` | 1 MB | 最大 Chunk |
| `--hash` | sha256 | 强哈希算法 |
| `--tracker-port` | 8080 | 写入种子的 Tracker 端口 |
| `--announce-ip` | 自动检测 | 多网卡时手动指定 |

---

### 3b. P2P 有线协议

#### 3b.1 连接模型

- 传输层：TCP
- 默认端口：`16889`
- 并发模型：每 Peer 一个 TCP 连接，全双工，Pipeline 请求
- 连接数：每节点最多 60 个 Peer 连接

#### 3b.2 握手 (67 字节)

```
发起方 → 接收方:

┌──────────────────────────────────────────────────────┐
│ 协议标识    char[19]  "thinBT Protocol\x00\x00\x00\x00"│  固定 19 字节
├──────────────────────────────────────────────────────┤
│ Reserved    uint8[4]  保留，填 0                      │
│ Speed       uint32    本机网卡协商速率 (Mbps)          │
│ InfoHash    uint8[20] .tseed 文件的 SHA-1             │
│ PeerID      uint8[20] 随机生成的本机标识               │
└──────────────────────────────────────────────────────┘

Speed 取值: 100 = 百兆, 1000 = 千兆, 2500 = 2.5G, 10000 = 万兆
```

#### 3b.3 消息格式

每条消息统一前缀：4 字节长度（Big-Endian，不含自身）+ 1 字节消息 ID。

```
┌──────────┬──────────┬────────────────────┐
│ length   │ msg_id   │ payload            │
│ uint32   │ uint8    │ length-1 字节      │
└──────────┴──────────┴────────────────────┘
```

#### 3b.4 消息类型

| ID | 名称 | Payload | 说明 |
|---|---|---|---|
| 0 | `choke` | 无 | 暂停上传 |
| 1 | `unchoke` | 无 | 恢复上传 |
| 2 | `interested` | 无 | 对对方数据感兴趣 |
| 3 | `not_interested` | 无 | 不感兴趣 |
| 4 | `have` | `chunk_index:4` | 宣布新拥有某 Chunk |
| 5 | `bitfield` | `bitfield:n` (变长) | 位图，标记已拥有的 Chunk |
| 6 | `request` | `index:4 + begin:4 + length:4` | 请求子块数据 |
| 7 | `piece` | `index:4 + begin:4 + data:n` | 传输子块数据 |
| 8 | `cancel` | `index:4 + begin:4 + length:4` | 取消请求（Endgame 冗余清理） |
| 9 | `pex` | `op:1 + count:2 + peers[]` | Gossip 节点交换 |

#### 3b.5 PEX 消息格式 (msg_id=9)

```
PexPeer (8 字节，严格对齐):

struct PexPeer {
    uint32_t ip;       // IPv4 地址 (网络字节序)
    uint16_t port;     // P2P 端口 (网络字节序)
    uint8_t  flags;    // 0x01=种子节点  0x02=千兆  0x04=支持加密
    uint8_t  reserved; // 对齐到 8 字节
};

PEX 消息 payload:
  op        uint8        0x00=全量(首次握手)  0x01=增量(Delta)
  count     uint16       本消息携带的 Peer 数量
  peers[]   PexPeer[n]
```

**设计理由**：
- **去掉 speed 字段**：网络中第三方报告的速率对接收者无参考意义——A→B 的链路质量和 C→B 完全不同。速率必须由各节点自行连接后被动探测，不可信任 Gossip 路由的"主观速率"。
- **flags 替代 speed**：传递拓扑元数据（种子节点/千兆/加密能力），接收方自行决定是否连接。
- **8 字节对齐**：避免 10 字节结构体在数组中产生 Padding 碎片，最大化 CPU 缓存行利用。

**Delta Updates 策略**：PEX 不广播全量 Peer 列表，只发送增量变化：

```
首次连接 → op=0x00（全量），携带所有已知 Peer
后续每 60 秒 → op=0x01（增量），仅携带:
  1. 最近 60 秒新连上的节点
  2. 最近 60 秒断开的节点（flags 最高位置 1 标记"已离开"）
接收端:
  - 新节点 → 尝试连接
  - 已离开标记 → 从本地 Peer 池清理
```

将 O(N) 的全局 Gossip 同步开销降为 O(1) 增量同步。

#### 3b.6 子块请求策略

| 参数 | 值 | 理由 |
|---|---|---|
| 最大子块大小 | 16 KB | BT 标准子块粒度 |
| Pipeline 深度 | **50 ~ 100（自适应）** | 局域网 RTT < 1ms，浅 Pipeline 导致 in-flight 数据不足，发完即干等 ACK |
| 超时时间 | **2 ~ 5 秒** | 局域网丢包极低，超时只意味对端已死。30 秒超时是公网思维，在局域网等同于"假死" |
| 策略 | **动态窗口（类 BBR）** | 只要对方回应够快就不断扩大窗口，直到测出对端 I/O 或带宽上限 |

**动态 Pipeline 算法**：

```
初始窗口: 16 个 Request (~256KB in-flight)
每 500ms 评估:
  本周期无超时 && 所有 Pending 平均延迟 < 2ms:
    peer.pipeline_cap = min(peer.pipeline_cap * 1.5, MAX_PIPELINE(100))
  出现超时或慢响应:
    peer.pipeline_cap = max(peer.pipeline_cap / 2, MIN_PIPELINE(8))
稳定态: 窗口在 [min, max] 之间跟随对端实际吞吐波动
```

**Fast Fail 机制**：

```
Request 发出 → 启动 3 秒定时器
  ├─ 3 秒内收到 Piece → 取消定时器，正常流程
  └─ 超时:
      → Cancel(chunk_index, begin, length) 发给该 Peer（尽力发送）
      → 从 pending 队列立即移除该子块
      → 将该子块重新标记为 MISSING
      → Scheduler 重新选 Peer 分配
      → 该 Peer 连续 3 次超时 → 断开连接，标记为僵尸节点
```

#### 3b.7 Choke/Unchoke 策略

**动态 Unchoke 槽位**（打破固定 4 个的公网思维）：

```
默认槽位: 4
额外槽位: 每 10MB/s 空闲上行带宽 ≈ 2 个额外槽位
最大槽位: 20（防止 fd 过载）

示例:
  千兆上行已用 300Mbps，剩余 700Mbps
  → 额外槽位 = 70 / 10 * 2 = 14
  → 总槽位 = 4 + 14 = 18
```

**Anti-Starvation（防范百兆节点被饿死）**：

标准 Tit-for-Tat 在异构网络中导致"穷者越穷"——百兆节点因上行慢，永远进不了千兆节点的 Unchoke 前 4 名，最终被集体 Choke。修订后的分配策略：

```
每 10 秒评估周期:
  50% 槽位: 按下载速率排序（Tit-for-Tat）
  25% 槽位: Optimistic Unchoke（随机轮换，给新节点机会）
  25% 槽位: Anti-Starvation 槽位
    └─ 优先给 flags 标记为百兆且尚未被 Unchoke 的 Peer
    └─ 每轮强制轮换，确保每个百兆节点都有机会获取数据
```

---

### 3c. I/O 层与并发模型

#### 3c.1 核心认知：memcpy 到 mmap ≠ 磁盘 I/O

```
I/O 工作线程 memcpy(base + offset, data, len)
        │
        ▼
  写入 Page Cache（内存操作，纳秒级）
        │
        ▼
  内核 flusher 线程组（完全独立于应用进程）
        │
        ▼
  收集脏页 → 按 LBA 排序 → 电梯算法 → 顺序大块写入磁盘
```

**结论**：应用层多线程并发 `memcpy` 到 mmap 不同偏移，不会导致磁盘随机写。磁盘的物理写入顺序由内核 I/O 调度器统一优化。I/O 线程数无需限制为单线程。

#### 3c.2 线程模型

```
                        ┌──────────────────────┐
    网络事件循环          │  epoll / IOCP        │
    (主线程, 1)           │  ┌─────────────────┐ │
                          │  │ TCP Accept      │ │
                          │  │ Peer Read/Write │ │
                          │  │ IPC Listener    │ │
                          │  │ Tracker Client  │ │
                          │  │ Scheduler       │ │  ← 单线程，无需 atomic
                          │  └────────┬────────┘ │
                          └───────────┼──────────┘
                                      │ 收包完成后 push
                                      ▼
                        ┌──────────────────────┐
    I/O 线程池            │  SPSC 无锁队列 x N    │
    (4~8 线程,            │  ┌─────────────────┐ │
     按 CPU 核数)         │  │ memcpy→mmap     │ │
                          │  │ atomic 标记完成  │ │
                          │  └────────┬────────┘ │
                          └───────────┼──────────┘
                                      │ Chunk 完成时 push
                                      ▼
                        ┌──────────────────────┐
    计算线程池            │  MPMC 无锁队列        │
    (2~4 线程)            │  ┌─────────────────┐ │
                          │  │ SHA-256 校验    │ │
                          │  └────────┬────────┘ │
                          └───────────┼──────────┘
                                      │ 校验结果 push (SPSC)
                                      ▼
                        ┌──────────────────────┐
    主线程回调            │  更新位图              │
                          │  Have 消息广播        │
                          │  Scheduler 触发新请求 │
                          └──────────────────────┘
```

#### 3c.3 线程职责与并发安全

| 线程 | 数量 | 职责 | 安全机制 |
|---|---|---|---|
| **网络线程** (主) | 1 | epoll 事件循环，TCP 收发，协议解析，Scheduler 调度 | 单线程，无竞争 |
| **I/O 线程池** | `std::thread::hardware_concurrency() / 2`，上限 8 | 从 SPSC 队列取 PieceTask → `memcpy` 到 mmap → 原子标记 | 不同 Piece 操作不同内存偏移；同一 slot 靠 `fetch_or` 拦截重复 |
| **计算线程池** | 2~4 | SHA-256 校验已完成的 Chunk | 只读 Chunk 数据 + 结果通过 SPSC 写回主线程 |
| **Tracker 定时器** | 主线程内 | 定期 announce 心跳、清理过期 Peer | 单线程内执行 |

#### 3c.4 无锁队列选型

| 队列 | 用途 | 实现 |
|---|---|---|
| PieceTask 队列 | 网络线程 → I/O 线程池 | **SPSC**，每 I/O 线程一个独立队列。路由: `(chunk_idx ^ slot_idx) % num_io_threads` |
| Verify 队列 | I/O 线程池 → 计算线程池 | **MPMC**，`std::atomic` + CAS |
| 回调队列 | 计算线程池 → 主线程 | **SPSC**，如 `moodycamel::ReaderWriterQueue` |

**SPSC 路由消偏设计**：

```
路由公式: (chunk_idx ^ slot_idx) % num_io_threads

理由:
  Endgame 阶段全网抢夺同一 Chunk 时，若用 chunk_idx % N 路由，
  该 Chunk 的所有 sub-block 会堆积在同一 I/O 线程的 SPSC 队列上，
  其他线程空闲。异或 slot_idx 后，同一 Chunk 的不同 sub-block
  被均匀打散到所有 I/O 线程，消除热点倾斜。
  
  同时，不同 slot 操作 Chunk 内不同偏移，memcpy 和原子操作依然无锁安全。
```

#### 3c.5 磁盘预分配与 mmap 预热

**磁盘预分配**：

```
下载开始前:
  Linux:   fallocate(fd, 0, 0, file_size)  → 文件系统分配连续物理块
  Windows: SetFileValidData() + SetEndOfFile() → NTFS 预分配
  目的:    避免按需分配导致的碎片，保证后续 mmap 写入落在连续扇区
```

**mmap 预热（Page Fault 消解）**：

`memcpy` 到 mmap 区域首次写入某页时，会触发软缺页中断（Soft Page Fault）——内核需要分配物理页、清零、建立映射，耗时数微秒到数十微秒。大量 Piece 并发写入时，密集的缺页中断会严重吃掉 CPU。

| 平台 | 方法 | 策略 |
|---|---|---|
| Linux | `mmap(..., MAP_POPULATE)` 或 `madvise(..., MADV_WILLNEED)` | 默认**滑动窗口预热**——仅对 `[window_start, window_start + 1GB]` 调用 `MADV_WILLNEED`，随下载窗口滑动逐步预热。避免 50GB 一次性 Populate 占用全部物理内存 |
| Windows | `VirtualAlloc(..., MEM_COMMIT)` + `PrefetchVirtualMemory()` | 预提交物理页 |

**设计决策**：I/O 线程的主要耗时不在 `memcpy` 本身，而在首次写入导致的软缺页中断。多 I/O 线程的意义之一——线程 A 陷入缺页中断被内核挂起时，线程 B/C/D 继续处理其他 Piece，掩盖延迟。

#### 3c.6 零拷贝上传

| 平台 | 系统调用 | 说明 |
|---|---|---|
| Linux | `sendfile(socket_fd, file_fd, &offset, length)` | 内核态: Page Cache → Socket Buffer → 网卡 DMA，数据不经用户态，CPU 零参与 |
| Windows | `TransmitFile(socket, file_handle, ...)` + IOCP | 等效的内核态零拷贝，完美融入 IOCP 异步模型 |

**fallback**：若 Chunk 尚未落盘（仍在 Page Cache 脏页），直接用 `write()` 从 mmap 地址发送，虽然多一次拷贝但保证数据正确。

#### 3c.7 ChunkAssembler：海量乱序子块的无锁拼装

```cpp
class ChunkAssembler {
public:
    // 初始化：mmap 映射到文件对应偏移，base 指向目标地址
    void init(uint8_t* mmap_base, uint32_t chunk_size, uint32_t sub_block_size);

    // Piece 消息到达时的回调（被 I/O 线程池调用）
    void on_piece(uint32_t begin, const uint8_t* data, uint32_t len);

    // 是否全部完成
    bool is_complete() const {
        return pending_count_.load(std::memory_order_acquire) == 0;
    }

private:
    uint8_t* base_;                                  // mmap 映射的目标地址
    uint32_t chunk_size_;
    uint32_t sub_block_size_;
    uint32_t total_slots_;
    std::vector<std::atomic<uint32_t>> completed_mask_;  // 位图
    std::atomic<uint32_t> pending_count_;                 // 待完成子块计数
};

void ChunkAssembler::on_piece(uint32_t begin, const uint8_t* data, uint32_t len) {
    uint32_t slot      = begin / sub_block_size_;
    uint32_t word      = slot / 32;
    uint32_t bit_mask  = 1u << (slot % 32);

    // 写入 mmap 映射区（纯内存操作，纳秒级）
    memcpy(base_ + begin, data, len);

    // fetch_or 返回修改前的值 → 判断是否首次完成该 slot
    uint32_t old_mask = completed_mask_[word].fetch_or(
        bit_mask, std::memory_order_release);

    if ((old_mask & bit_mask) == 0) {
        // 首次完成：安全递减
        if (pending_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // 最后一个子块 → Chunk 完整，通过 SPSC 通知主线程
            notify_chunk_complete();
        }
    }
    // else: 超时后旧 Peer 延迟到达的重复数据
    //       数据覆盖写入无副作用，计数器不递减，状态安全
}
```

**设计要点**：

| 问题 | 解决方案 |
|---|---|
| 两个线程同时写入同一 slot？ | 不可能——Scheduler 保证同一时刻同一子块只发给一个 Peer（超时重分配前先从 pending 中移除旧请求） |
| 两个 Piece 同时到达不同 slot？ | `memcpy` 写入不同偏移（`base + begin`），内存不重叠，无锁安全 |
| 超时重分配后旧 Peer 的 Piece 延迟到达？ | `fetch_or` 返回值判断——若该 slot 已被新 Peer 完成，`old_mask & bit_mask != 0`，计数器不递减，重复数据直接忽略 |
| Chunk 何时标记完成？ | `pending_count.fetch_sub(1) == 1` 时当前线程是最后一个完成者，单线程安全地执行完成回调 |
| 内存泄漏？ | 无动态分配。`base_` 指向 mmap 文件映射区，由 OS Page Cache 管理 |

---

### 3d. 调度层

#### 3d.1 三阶段状态机

```
    [init]
       │
       ▼
    ┌────────┐
    │ NORMAL │  ← Rarest First 选块 + 速率加权选 Peer
    └───┬────┘
        │ missing_chunks < 128（绝对阈值，约 500MB 对于 128KB Chunk）
        ▼
    ┌────────┐
    │ENDGAME │  ← 对"饥饿请求"向 2~3 个最闲 Peer 发冗余请求
    └───┬────┘
        │ missing_chunks == 0
        ▼
    ┌────────┐
    │  DONE  │
    └────────┘
```

**为什么 Endgame 不用百分比（如 5%）**：50GB 的 5% = 2.5GB，此时广播式 Endgame 会产生 150GB+ 冗余流量，瞬间打爆交换机缓冲区。使用绝对 Chunk 数（< 128）作为阈值，Endgame 窗口始终可控在 ~500MB 以内。

#### 3d.2 NORMAL 阶段

**事件驱动的 availability 维护**（O(1) 增量更新，替代 O(N×M) 定时全量遍历）：

```
收到 Peer BITFIELD 消息时:
  for each bit=1 in bitfield:
    availability_[chunk_idx]++

收到 Peer HAVE 消息时:
  availability_[chunk_idx]++

Peer 断开时:
  for each chunk owned by peer:
    availability_[chunk_idx]--

availability_ 永远是实时快照，调度器直接 O(1) 读取，消灭 CPU 空转。
```

**选块逻辑**：

```
100ms 调度周期:
  1. 收集所有 MISSING 状态的 Chunk
  2. 按 availability_[chunk] 升序排列（最稀缺优先）
  3. 取前 batch_size 个，各调用 select_best_peer()
  4. 向选中的 Peer 发出 Request
```

**Peer 选择算法（先过滤，再打分）**：

```cpp
PeerSlot* select_best_peer(uint32_t chunk_idx) {
    PeerSlot* best = nullptr;
    int best_score = INT_MIN;

    for (auto& p : peer_slots_) {
        // ═══════════════════════════════════
        // 硬过滤条件（一票否决，不可降级为加分项）
        // ═══════════════════════════════════
        if (!p.remote_bitfield[chunk_idx]) continue;  // 没有数据 → 跳过
        if (p.am_choking)                   continue;  // 被 Choke → 跳过
        if (p.consecutive_timeouts >= 3)    continue;  // 僵尸节点 → 跳过

        // ═══════════════════════════════════
        // 加权打分
        // ═══════════════════════════════════
        int score = 0;

        // 速率匹配
        if (p.link_speed_mbps >= 1000 && local_cap_ >= 1000)
            score += 50;   // 千兆↔千兆：强偏好
        else if (local_cap_ >= 1000)
            score += 5;    // 千兆→百兆：弱偏好，降级使用

        // 负载均衡
        score -= p.pending_requests * 3;

        // RTT 微调
        score -= p.rtt_us / 2000;  // 1ms RTT = -0.5 分

        if (score > best_score) {
            best_score = score;
            best = &p;
        }
    }
    return best;
}
```

**设计理由**：`bitfield[chunk]` 是硬过滤条件而非加分项。若将其作为 +10 加分项，一个千兆低负载但没有该 Chunk 的 Peer 可能靠其他项总分胜出 → Request 发给错误的人 → 永久卡死。

#### 3d.3 ENDGAME 阶段（精准补漏）

**核心原则**：Endgame 不是"狂轰滥炸"，而是"精准补漏"。

```
触发条件: missing_chunks_count < 128

行为:
  只对"饥饿请求"触发 Endgame:
    扫描所有 DOWNLOADING 状态的 Chunk:
      for sub in chunk.pending_sub_blocks():
        if (now - sub.request_time) > 2 * peer.rtt_avg:  // 饥饿判定

          // 找持有该 Chunk 且最空闲的 Peer
          candidates = filter_peers(
            has_chunk    = true,
            is_choked    = false,
            timeouts     < 3
          )
          sort candidates by pending_requests ascending

          // 只向最闲的 2 个额外 Peer 发冗余请求，严禁广播
          for peer in candidates[:2]:
            issue_request(peer, chunk.idx, sub.begin, sub.length)
            sub.duplicate_count++
```

**禁止事项**：
- **禁止向所有 Peer 广播**：59 个 Peer × 16KB = ~1MB 冗余流量/子块，Cancel 指令跑不赢已在网卡队列中的数据包
- **禁止对所有缺失 Chunk 同时 Endgame**：`max_concurrent_endgame_chunks = 32`

#### 3d.4 Scheduler 数据结构

```cpp
class Scheduler {
public:
    void on_bitfield(PeerSlot& peer, const std::vector<uint8_t>& bitfield);
    void on_have(PeerSlot& peer, uint32_t chunk_idx);
    void on_peer_disconnect(PeerSlot& peer);
    void process_completions();  // 处理 I/O 线程回传的完成通知
    void tick();                 // 100ms 调度周期

private:
    // 纯单线程访问（epoll 主线程），无需 any atomic
    std::vector<ChunkState> chunk_states_;    // MISSING/REQUESTED/DOWNLOADING/COMPLETE
    std::vector<uint32_t>  availability_;     // 事件驱动增量维护
    std::vector<PeerSlot>  peer_slots_;

    Phase phase_ = NORMAL;
    uint32_t missing_count_ = 0;

    // I/O 线程池 → 主线程 的 SPSC 完成通知队列
    moodycamel::ReaderWriterQueue<ChunkCompleteMsg> completed_queue_;
};
```

**完成事件处理**：

```cpp
void Scheduler::process_completions() {
    ChunkCompleteMsg msg;
    while (completed_queue_.try_dequeue(msg)) {
        chunk_states_[msg.chunk_idx] = COMPLETE;
        missing_count_--;

        // 清理 Endgame 冗余请求：向还在传该 Chunk 的其他 Peer 发 Cancel
        for (auto& sub : msg.pending_sub_blocks) {
            for (auto& peer : sub.issued_peers) {
                if (peer != msg.winning_peer) {
                    peer.send_cancel(msg.chunk_idx, sub.begin, sub.length);
                }
            }
        }

        // 向所有 Peer 广播 Have
        broadcast_have(msg.chunk_idx);

        // 阶段切换
        if (missing_count_ == 0) {
            phase_ = DONE;
        } else if (missing_count_ < 128) {
            phase_ = ENDGAME;
        }
    }
}
```

#### 3d.5 Peer 调度槽位

```cpp
struct PeerSlot {
    uint32_t pending_requests = 0;             // 当前 in-flight Request 数
    uint32_t consecutive_timeouts = 0;           // 连续超时计数
    uint32_t rtt_us = 0;                        // 最近 RTT（微秒）
    uint32_t link_speed_mbps = 0;               // 本连接实测速率（非握手自报值）
    uint32_t pipeline_cap = 16;                  // 动态 Pipeline 上限
    uint32_t link_speed_reported = 0;            // 握手时自报的网卡速率
    bool     am_choking = true;
    std::vector<bool> remote_bitfield;           // 对方拥有的 Chunk 位图
};
```

---

### 3e. Tracker 服务

#### 3e.1 协议

- 传输层：TCP + 单行 JSON（`\n` 分帧），非 HTTP
- 内置于 `thinbtd`，不单独部署

**客户端 → Tracker**：

```json
{"op":"announce","info_hash":"<40 hex>","port":16889,"speed_mbps":1000}
```

**Tracker → 客户端**：

```json
{"interval":30,"peers":[{"ip":"192.168.1.10","port":16889,"flags":3}]}
```

**错误响应**：

```json
{"error":"message"}
```

#### 3e.2 心跳与清理

- 默认 announce 间隔：30 秒
- Peer 超时：90 秒未 announce 则清理
- 返回的 Peer 列表按 flags 千兆优先排序

#### 3e.3 0 Peer 重试策略

announce 成功但 peers 为空时（下载端先于做种端完成 announce 的时序问题），每 30 秒重试，上限 240 次（2 小时）。耗尽后任务状态为 `waiting`。

#### 3e.4 Gossip/PEX 容灾

```
Tracker 可用时:
  PEX 作为补充发现渠道，增量交换最近 60 秒的拓扑变化

Tracker 不可用时:
  PEX 成为唯一发现机制，已有连接的节点间通过 Gossip 维持拓扑
  局限性: 新节点无法加入（Trakcer 离线时没有引导首次 Peer 列表）
  已有节点: 继续工作，不受影响
```

#### 3e.5 启动参数

```bash
thinbtd [--ipc-port 16888] [--tracker-port 8080] [--p2p-port 16889]
```

---

### 3f. 命令行接口 (CLI)

#### 3f.1 通信协议

- Daemon 监听：`127.0.0.1:16888` (TCP)
- 消息格式：JSON 对象，`\n` 分隔

```json
// 请求
{"cmd":"<command>","args":{...}}

// 响应
{"status":"ok","data":{...}}
{"status":"error","error":"error message"}
```

#### 3f.2 子命令

| 命令 | 说明 | 是否 IPC |
|---|---|---|
| `tbt make <file> [options]` | 生成种子文件 | 否（CLI 本地执行） |
| `tbt info <.tseed>` | 查看种子信息 | 否 |
| `tbt seed <.tseed> <file>` | 启动做种任务 | 是 |
| `tbt add <.tseed> [save_path]` | 添加下载任务 | 是 |
| `tbt update <new.tseed> <new_file> <old.tseed> <old_file>` | 增量更新 | 是 |
| `tbt list` | 查询所有任务进度 | 是 |
| `tbt peers <task_id>` | 查看 Peer 详情 | 是 |
| `tbt remove <task_id>` | 删除任务 | 是 |

#### 3f.3 关键响应格式

**list 响应**：

```json
{
  "status": "ok",
  "data": {
    "tasks": [{
      "task_id": "a1b2c3d4",
      "state": "downloading",
      "progress": 0.4521,
      "bytes_done": 24288000000,
      "speed_mib_s": 118.5,
      "file_path": "/data/vm.qcow2",
      "seed_path": "/share/vm.tseed",
      "started_at": "2026-06-09T08:00:00Z",
      "finished_at": ""
    }]
  }
}
```

**状态值**：

| state | 说明 |
|---|---|
| `seeding` | 做种中 |
| `downloading` | 下载中 |
| `complete` | 成功完成 |
| `error` | 失败 |
| `waiting` | Tracker 可达但无 Peer（重试耗尽） |

**speed_mib_s**：传输中为近期 MiB/s 的指数移动平均（EMA）；任务完成后改写为整次任务平均 MiB/s（`file_size / 总耗时`），便于运维对比。

---

### 3g. 增量更新流程

#### 3g.1 整体流程

```
1. 加载新旧两个 .tseed 文件
2. 扫描本地旧文件（FastCDC），构建 {sha256 → [local_offsets]} 哈希索引
3. 遍历新种子每个 Chunk:
   命中（sha256 匹配） → copy_file_range/reflink 移动到新偏移 → 标记 HAVE
   未命中 → 标记 MISSING，进入 P2P 下载
4. 新文件 truncate 到目标大小
5. 旧文件中有但新文件中未引用的块 → fallocate(FALLOC_FL_PUNCH_HOLE) 释放空洞
6. 进入 P2P 下载缺失 Chunk
```

#### 3g.2 跨平台文件操作

| 操作 | Linux | Windows |
|---|---|---|
| Chunk 零拷贝移动 | `copy_file_range()` | `FSCTL_DUPLICATE_EXTENTS_TO_FILE` |
| 释放空洞 | `fallocate(FALLOC_FL_PUNCH_HOLE)` | `FSCTL_SET_ZERO_DATA` |
| 文件截断 | `ftruncate()` | `SetFileInformationByHandle(FileEndOfFileInfo)` |
| 磁盘预分配 | `fallocate(0, 0, size)` | `SetFileValidData()` |

---

## 4. 非功能需求

### 4.1 性能指标

| 指标 | 目标值 |
|---|---|
| CDC 扫描速度 | > 500 MB/s |
| 千兆网络吞吐 | > 900 Mbps（单任务） |
| 内存占用 | < 200 MB（含 50GB 任务位图） |
| CPU 空闲占用 | < 1% |
| 并发 Peer 数 | 60+ |

### 4.2 可靠性

- 每个 Chunk 写入后按 `.tseed` 中的 SHA-256 校验；默认不在任务结束时对整文件再做 SHA-256 全量扫描（缩短收尾时间，如需可加选项）
- 断点续传：任务中断后重启，扫描本地文件恢复进度
- Peer 断开后自动重连，Tracker 不可用时降级为 PEX 静态拓扑
- Chunk SHA-256 校验失败 → 标记为 MISSING → Scheduler 自动重新下载

### 4.3 跨平台

- 支持平台：Linux (kernel 3.10+)、Windows 10/Server 2016+
- 编译工具链：MSVC 2019+ / MinGW-w64 (Windows)、GCC 9+ / Clang 10+ (Linux)
- IPC 使用 TCP（而非 Unix Socket/Named Pipe），最大化代码复用
- 使用 `#ifdef _WIN32` / `#ifndef _WIN32` 隔离平台代码

### 4.4 设计约束

- **不与 BitTorrent 兼容**：自定义种子格式和有线协议，固定 Piece 切分与 CDC 动态分块天然矛盾
- **不实现加密传输**：局域网环境，数据为教学镜像，加密消耗 CPU
- **单文件任务模式**：每个 `.tseed` 仅描述一个文件（虚拟机镜像本身就是单文件）
- **不支持公网穿透**：场景限定局域网，不做 NAT 打洞

---

## 5. 目录结构

```
thinbt/
├── CMakeLists.txt
├── README.md
├── docs/
│   └── thinBT-design.md
├── src/
│   ├── common/
│   │   ├── platform.hpp        // #ifdef _WIN32 平台隔离
│   │   ├── file_util.hpp       // mmap/sendfile/fallocate 跨平台封装
│   │   ├── file_util.cpp
│   │   ├── net_util.hpp        // socket helpers
│   │   ├── net_util.cpp
│   │   └── hash.hpp/cpp        // SHA-256, xxHash
│   ├── cdc/
│   │   ├── fastcdc.hpp
│   │   └── fastcdc.cpp         // FastCDC 分块算法
│   ├── seed/
│   │   ├── tseed.hpp           // TSeedHeader, ChunkEntry 结构体
│   │   ├── seed_reader.cpp     // .tseed 解析
│   │   └── seed_writer.cpp     // .tseed 生成
│   ├── daemon/
│   │   ├── main.cpp            // 入口
│   │   ├── ipc_server.hpp/cpp  // TCP/JSON IPC
│   │   ├── task_manager.hpp/cpp
│   │   ├── peer_manager.hpp/cpp  // Peer 连接池 + PEX/Gossip
│   │   ├── peer_session.hpp/cpp  // 单 Peer 收发状态机
│   │   ├── scheduler.hpp/cpp     // NORMAL/ENDGAME 调度
│   │   ├── chunk_assembler.hpp/cpp  // 无锁子块拼装（核心）
│   │   ├── io_worker.hpp/cpp    // I/O 线程池
│   │   ├── tracker_client.hpp/cpp
│   │   └── tracker_server.hpp/cpp
│   └── cli/
│       ├── tbt.cpp              // CLI 入口
│       └── cli_commands.cpp
├── tests/
│   ├── test_chunk_assembler.cpp  // 无锁拼装单元测试
│   ├── test_scheduler.cpp
│   └── test_fastcdc.cpp
└── third_party/
    ├── moodycamel/              // ReaderWriterQueue (header-only)
    ├── yyjson/                  // JSON 解析 (header-only)
    └── asio/                    // Asio 网络库 (header-only)
```

---

## 6. 使用场景

### 场景 1：首次全量分发

```bash
# 教师机 (192.168.1.10)
thinbtd --tracker-port 8080 &
tbt make /mnt/vm.qcow2 --output /share/vm.tseed --tracker-port 8080
tbt seed /share/vm.tseed /mnt/vm.qcow2

# 学生机 (所有机器)
scp user@192.168.1.10:/share/vm.tseed /tmp/
tbt add /tmp/vm.tseed /data/vm.qcow2
tbt list  # 监控进度
```

### 场景 2：镜像修改后增量更新

```bash
# 教师机（修改镜像后）
tbt make /mnt/vm_v2.qcow2 --output /share/vm_v2.tseed
tbt seed /share/vm_v2.tseed /mnt/vm_v2.qcow2

# 学生机（已拥有旧版本数据与旧种子）
tbt update /share/vm_v2.tseed /data/vm_v2.qcow2 \
           /share/vm_v1.tseed /data/vm_v1.qcow2
# reuse 阶段进度即上升 → P2P 仅拉取变更 Chunk
tbt list
```

---

## 7. 术语表

| 术语 | 定义 |
|---|---|
| CDC | Content-Defined Chunking，内容定义分块 |
| Chunk | 文件分块单元，由 CDC 算法动态切分（平均 128KB） |
| Sub-block / Piece | P2P 传输的最小单元（16KB），一个 Chunk 由多个 Piece 组成 |
| InfoHash | `file_sha256` + 所有 `ChunkEntry[]` 的 SHA-1（20 字节），用于 Tracker announce 与 P2P 握手，不包含可变元数据 |
| Task ID | Daemon 为每个任务生成的 8 字符小写十六进制 ID，用于 IPC 操作 |
| Peer | P2P 网络中的对等节点 |
| Tracker | 中心协调服务，维护 Peer 列表；内置于 `thinbtd`，TCP + 单行 JSON |
| PEX | Peer Exchange，Gossip 协议的节点交换消息 |
| SPSC | Single Producer Single Consumer，单生产者单消费者无锁队列 |
| MPMC | Multiple Producer Multiple Consumer，多生产者多消费者无锁队列 |
| thinbt:// | 种子中 Tracker 的 URI scheme，应用层非 HTTP |
| .tseed | thinBT 种子文件扩展名 |
