# thinBT 代码问题清单

**日期**：2026-06-09
**范围**：当前代码与 design.md 的一致性检查

---

## 🔴 严重问题（可能导致运行时错误或二进制不兼容）

### 1. Handshake 结构体缺少 `#pragma pack`

- **文件**：[src/daemon/protocol.hpp](src/daemon/protocol.hpp)
- **设计依据**：3b.2 握手格式定义为固定 67 字节
- **问题**：`Handshake` 结构体未加 `#pragma pack(push, 1)`。结构体布局为：
  ```
  protocol_id[19] (offset 0)
  reserved[4]     (offset 19)
  speed_mbps      (offset 23 — 不是 4 字节对齐！)
  info_hash[20]   (offset 27)
  peer_id[20]     (offset 47)
  ```
  `uint32_t speed_mbps` 需要 4 字节对齐，offset 23 不满足，编译器可能插入 1 字节 padding，导致实际大小为 68 字节。`serialize_handshake` 中 `memcpy(buf.data(), &h, 67)` 会漏掉最后 1 字节或读到错误偏移。
- **修复**：在 `struct Handshake` 前后加 `#pragma pack(push, 1)` / `#pragma pack(pop)`

### 2. PexPeer 结构体缺少 `#pragma pack`

- **文件**：[src/daemon/protocol.hpp](src/daemon/protocol.hpp)
- **设计依据**：3b.5 PexPeer 严格 8 字节对齐
- **问题**：虽然 4+2+1+1=8 天然无 padding，但无 `#pragma pack` 意味着依赖编译器默认行为，不同平台/编译器可能不同。`build_pex` 中 `memcpy(payload.data() + 3 + i * 8, &p, 8)` 假设 8 字节布局。
- **修复**：同 Handshake，加 pack pragma

### 3. FastCDC GEAR 表不完整

- **文件**：[src/cdc/fastcdc.cpp:9-14](src/cdc/fastcdc.cpp#L9-L14)
- **设计依据**：FastCDC 算法需要 256 项随机 64 位值用于滚动哈希
- **问题**：
  ```cpp
  static const uint64_t GEAR[256] = {
      0x9E3779B185EBCA87, 0x2C13A0F1B8D9E506, ..., 0x0F1E2D3C4B5A6978,
  };  // ← 仅 16 项，其余 240 项被零初始化
  ```
  `static_assert` 确认了数组大小为 256，但只提供了 16 个值。其余 240 项全为 0，这意味着 93.75% 的输入字节在滚动哈希中不产生有效扩散，严重降低分块质量。文件修改后可能产生大量假边界变更。
- **修复**：补全 256 项随机 Gear 值，或使用公式动态生成

---

## 🟡 中等问题（功能正确但偏离设计或有性能浪费）

### 4. Endgame 饥饿判定用 per-chunk 时间，非 per-sub-block

- **文件**：[src/daemon/scheduler.cpp:282](src/daemon/scheduler.cpp#L282)
- **设计依据**：3d.3 "扫描所有 DOWNLOADING 状态的 Chunk: for sub in chunk.pending_sub_blocks(): if (now - sub.request_time) > 2 * peer.rtt_avg"
- **问题**：
  ```cpp
  // 当前代码：所有 sub-block 统一用 chunk 首次请求时间判定
  if ((now_ms - chunk_req_time) <= hunger_threshold_ms) continue;
  ```
  `chunk_first_req_time_` 在第一个 sub-block 发出时记录，后续 sub-block 可能刚发出就被误判为"饥饿"，触发不必要的冗余请求。设计明确要求每个 sub-block 独立追踪 `request_time`。
- **修复**：为每个 sub-block 记录独立发出时间；或至少用"该 sub-block 最晚被任何 Peer 请求的时间"

### 5. `on_subblock_timeout` 将整个 Chunk 重置为 MISSING

- **文件**：[src/daemon/scheduler.cpp:341-352](src/daemon/scheduler.cpp#L341-L352)
- **设计依据**：3b.6 Fast Fail "将该子块重新标记为 MISSING"（子块，非整个 chunk）
- **问题**：
  ```cpp
  void Scheduler::on_subblock_timeout(uint32_t chunk_idx) {
      chunk_states_[chunk_idx] = ChunkState::MISSING;  // 整个 chunk 回退
      chunk_requested_end_[chunk_idx] = 0;
      std::fill(chunk_sub_done_[chunk_idx].begin(), chunk_sub_done_[chunk_idx].end(), false);
  }
  ```
  一个 16KB sub-block 超时 → 整个 chunk（可能 128KB~1MB）全部重来。已成功写入 mmap 的 sub-block 被废弃，浪费已消耗的带宽和 I/O。
- **修复**：只回退超时的 sub-block，维持 chunk 在 DOWNLOADING 状态，下一轮 tick 重新分配该 sub-block

### 6. 增量更新（cmd_update）缺少数据拷贝

- **文件**：[src/daemon/task_manager.cpp:339-340](src/daemon/task_manager.cpp#L339-L340)
- **设计依据**：3g.1 步骤 3 "命中 → copy_file_range/reflink 移动到新偏移"
- **问题**：
  ```cpp
  if (it != old_chunk_index.end()) {
      init_bf[i] = true;
      matched++;
      // TODO: copy_file_range/reflink 将旧文件数据移动到新偏移
      // 当前仅标记 bitmap，后续窗口完善实际数据拷贝
  }
  ```
  匹配的 Chunk 仅在 bitfield 中标记 HAVE，旧文件数据从未拷贝到新文件。结果：新文件中这些偏移区域全是 0，SHA-256 校验必然失败。
- **修复**：调用 `file_util::clone_range` 将旧文件数据复制到新文件对应偏移

### 7. 增量更新缺少文件操作

- **文件**：[src/daemon/task_manager.cpp](src/daemon/task_manager.cpp) `cmd_update`
- **设计依据**：3g 步骤 4-6（truncate、punch_hole、释放未被引用的块）
- **问题**：增量更新流程缺失以下步骤：
  - 新文件 truncate 到目标大小（`SegmentWriter::open` 已做 `ftruncate`/`fallocate`，但未按新种子大小）
  - 旧文件中有但新种子中未引用的块用 `fallocate(FALLOC_FL_PUNCH_HOLE)` 释放
- **修复**：补充完整流程

### 8. 链路速率硬编码 1000Mbps

- **文件**：[src/daemon/task_manager.cpp:91](src/daemon/task_manager.cpp#L91), L117, L191, L217 等
- **设计依据**：3b.2 Speed 取值 = 本机网卡协商速率
- **问题**：所有任务创建处 `local_speed_mbps` 硬编码为 `1000`：
  ```cpp
  task->scheduler->init(chunk_count, 1000, ...);
  task->peer_mgr = std::make_unique<PeerManager>(io_, ..., 1000, ...);
  ```
  百兆网卡的学生机也会报告 1000Mbps，导致 Choke 槽位计算、速率加权打分全部失真。
- **修复**：调用 `detect_link_speed_mbps()` 获取实际值，作为参数传入 `TaskManager`

### 9. 断点续传未实现

- **文件**：全局
- **设计依据**：4.2 "任务中断后重启，扫描本地文件恢复进度"
- **问题**：`SegmentWriter::open` 有保留已有文件的逻辑（`size_matches` 时不 truncate），但 `TaskManager::cmd_add` 没有任何恢复逻辑：
  - 不扫描已有文件的 Chunk 完成情况
  - 不重建 Scheduler 的 bitfield
  - 不恢复 `bytes_done` 进度
- **修复**：在 `cmd_add` 中增加断点续传逻辑：对已有文件做 FastCDC 扫描 → 比对 SHA-256 → 标记已完成 Chunk

---

## 🟢 轻微问题（不影响功能但降低一致性或可维护性）

### 10. Tracker URL 解析重复实现

- **文件**：[src/daemon/task_manager.cpp:499-511](src/daemon/task_manager.cpp#L499-L511) 和 [src/common/net_util.cpp:191-247](src/common/net_util.cpp#L191-L247)
- **问题**：两处独立实现了 `thinbt://` URL 解析逻辑。`net_util` 有完善的 `parse_tracker_url()`（含 IPv6 支持），`task_manager` 的 `tick_tracker_announce` 又手写了一套仅支持 IPv4 的解析。
- **修复**：`tick_tracker_announce` 改用 `parse_tracker_url`

### 11. Choke/Unchoke 消息绕过 `build_message`

- **文件**：[src/daemon/peer_manager.cpp:151-160](src/daemon/peer_manager.cpp#L151-L160)
- **问题**：
  ```cpp
  std::vector<uint8_t> msg(5);
  if (s->is_choked()) {
      uint32_t len_be = hton32(1); msg[4] = 0;
      memcpy(msg.data(), &len_be, 4);
  } else {
      uint32_t len_be = hton32(1); msg[4] = 1;
      memcpy(msg.data(), &len_be, 4);
  }
  ```
  手动构造消息，而非使用 `build_message(P2PMsgId::CHOKE/UNCHOKE, ...)`。协议层已有统一构造器，应复用。
- **修复**：直接调用 `build_message`

### 12. Choke 槽位公式使用总速率而非空闲带宽

- **文件**：[src/daemon/peer_manager.cpp:123](src/daemon/peer_manager.cpp#L123)
- **设计依据**：3b.7 "每 10MB/s 空闲上行带宽 ≈ 2 个额外槽位"
- **问题**：
  ```cpp
  uint32_t slots = std::min(4u + local_speed_mbps_ / 100 * 2, 20u);
  ```
  设计说的是**空闲**上行带宽，即总带宽减去当前上传使用量。代码直接用总链路速率，空闲时没问题，但实际有上传负载时会高估槽位数。
- **修复**：计算 `idle_speed = local_speed_mbps_ - current_upload_mbps`，再据此算槽位

### 13. `process_completions` 的 Cancel 不区分 winning_peer

- **文件**：[src/daemon/scheduler.cpp:323](src/daemon/scheduler.cpp#L323)
- **设计依据**：3d.3 完成事件处理伪代码 "if (peer != msg.winning_peer)"
- **问题**：
  ```cpp
  send_cancel_for_chunk(msg.chunk_idx, UINT32_MAX); // 取消所有 peer，不排除任何
  ```
  设计意图是保留 winning_peer 的活跃记录。因 Chunk 已完成，功能上没问题，但不如设计精确。
- **修复**：传入 `msg.winning_peer_slot` 作为 exclude_slot

### 14. `uint32_t` slot_id 用 `UINT32_MAX` 做哨兵值

- **文件**：[src/daemon/scheduler.cpp:111](src/daemon/scheduler.cpp#L111)
- **问题**：`select_best_peer` 返回 `UINT32_MAX` 表示"无可用 Peer"。理论上 60 个 Peer 上限不会触发，但类型上哨兵值与合法值冲突。
- **修复**：改用 `std::optional<uint32_t>` 或单独维护一个 `static constexpr uint32_t NO_PEER = UINT32_MAX`

### 15. IPC list 响应缺少 `started_at` / `finished_at`

- **文件**：[src/daemon/ipc_server.cpp:88-100](src/daemon/ipc_server.cpp#L88-L100)
- **设计依据**：3f.3 list 响应格式含 `started_at` 和 `finished_at`
- **问题**：`TaskInfo` 中有 `started_at` 和 `finished_at` 字段，但 `ipc_server` 的 list 序列化未输出这两个字段。
- **修复**：补全 JSON 序列化

### 16. 未实现 `sendfile` 零拷贝上传

- **文件**：[src/daemon/peer_session.cpp:174-184](src/daemon/peer_session.cpp#L174-L184)
- **设计依据**：3c.6 零拷贝上传使用 sendfile
- **问题**：代码使用 `pread()` 代替 `sendfile()`，注释说明了原因（Asio 事件循环中阻塞调用不可行）。功能正确，但偏离设计。`file_util.cpp` 中的 `sendfile_zero_copy` 封装未被使用。
- **修复**：评估用 `splice(2) + SPLICE_F_NONBLOCK + pipe` 实现真正的异步零拷贝；或在设计文档中更新此决策

### 17. `PeerSession` 未发送 NOT_INTERESTED

- **文件**：[src/daemon/peer_session.cpp](src/daemon/peer_session.cpp)
- **设计依据**：3b.4 消息类型包含 NOT_INTERESTED(3)
- **问题**：收到 UNCHOKE 时有 `send_interested()` 逻辑，但没有对应的"当不再需要对方数据时发送 NOT_INTERESTED"逻辑。`send_not_interested()` 方法存在但从未被调用。
- **修复**：在 Scheduler 中检测：当某 Peer 的 bitfield 与本地 missing chunks 无交集时，调用 `send_not_interested()`

### 18. `tick_endgame` 中剩余 sub-block 计数使用 `std::count`

- **文件**：[src/daemon/scheduler.cpp:236-237](src/daemon/scheduler.cpp#L236-L237)
- **问题**：
  ```cpp
  uint32_t done = static_cast<uint32_t>(
      std::count(chunk_sub_done_[a].begin(), chunk_sub_done_[a].end(), true));
  ```
  每次 tick 都 O(n) 遍历所有 ENDGAME chunk 的所有 sub-block 的完成位图。可用 `chunk_requested_end_` 结合 `total - done_here` 代替，或在 sub-block 完成时维护计数器。
- **修复**：维护 `per_chunk_done_count_` 计数器，O(1) 获取

### 19. `MappedFile` 未被增量更新调用

- **文件**：[src/common/file_util.hpp](src/common/file_util.hpp)
- **问题**：`MappedFile` 提供了 `punch_hole()`、`clone_range()` 等完善封装，但增量更新流程 (`cmd_update`) 未使用它们。`clone_range` 在 Windows 下返回 false 且无 fallback。
- **修复**：在增量更新中调用这些封装；Windows 下实现 mmap+memcpy fallback

### 20. `handle_pex_msg` 中 `flags & 0x80` 判断 "已离开"

- **文件**：[src/daemon/peer_session.cpp:210](src/daemon/peer_session.cpp#L210)
- **设计依据**：3b.5 "flags 最高位置 1 标记'已离开'"
- **问题**：
  ```cpp
  bool is_left = (p.flags & 0x80) != 0;
  ```
  设计说的是"flags 最高位"，但 PexPeer 的 flags 是 `uint8_t`，0x80 确实是最高位。此处理正确。但 `build_pex` 中设置离开标记时：
  ```cpp
  p.flags |= 0x80; // preserve original flags, set "left" bit
  ```
  这里只有 disconnects 列表中的 peer 会设置此位，但 `build_pex` 中 connects 的 peer flags 是 0。如果 connects 的 peer 原是千兆节点（flags=0x02），断开后重连时 flags 会丢失千兆标记。不过这不是严重问题，因为 disconnect 时 flags 通常已经不需要了。
- **修复**：`recent_connects_` 应保存完整 flags，而非在 `build_pex` 中写死 0

---

## 📊 统计汇总

| 严重度 | 数量 | 说明 |
|--------|------|------|
| 🔴 严重 | 3 | 二进制兼容性 + 算法正确性 |
| 🟡 中等 | 6 | 功能不完整或性能浪费 |
| 🟢 轻微 | 11 | 代码质量/一致性 |

**总计**：20 个问题
