# 窗口 7 提示词 — 真机部署与性能测试

---

你在操作 thinBT 项目，工作目录 `/home/thinbt/new thinbt`。

## 项目背景

窗口 1-6 已完成，代码能编译、CLI 能创建种子、thinbtd 能启动完整的事件循环、Tracker TCP 可访问、loopback 测试通过。现在要在真实教室环境下部署验证。

## 测试环境

4 台教室测试机器：

| 主机 | IP | 角色 |
|------|-----|------|
| 教师机 | 192.168.177.56 | Tracker + Seed |
| 学生机 1 | 192.168.177.177 | Peer |
| 学生机 2 | 192.168.177.74 | Peer |
| 学生机 3 | 192.168.177.41 | Peer |

网络：千兆交换机，教师机只有 1 个千兆上行端口，60 台学生机共享。

## ⚠️ 环境信息

- SSH 密钥：Ed25519 (hjx1011)，配置在 `/root/.claude/projects/-home-thinbt/memory/ssh-key.md`
- 测试用 UOS ISO 文件路径，参考 `/root/.claude/projects/-home-thinbt/memory/test-configuration.md`

---

## 任务 7a：编译产物部署

### 目标

将 thinbtd 和 tbt 二进制部署到所有 4 台机器。

### 步骤

1. 在本地编译 Release 版本：
   ```bash
   cd "/home/thinbt/new thinbt/build"
   cmake .. -DCMAKE_BUILD_TYPE=Release
   make -j$(nproc)
   ```

2. 将 `thinbtd`、`tbt` 和测试文件 scp 到所有机器：
   ```bash
   for ip in 56 177 74 41; do
       scp build/thinbtd build/tbt root@192.168.177.$ip:/usr/local/bin/
   done
   ```

3. 如果需要 .tseed 文件，先在教师机上用 `tbt make` 生成，再分发：
   ```bash
   tbt make /path/to/UOS.iso -o /path/to/UOS.iso.tseed
   scp /path/to/UOS.iso.tseed root@192.168.177.56:/tmp/
   ```

---

## 任务 7b：单节点功能验证

### 目标

确认 thinbtd 在所有机器上能正常启动。

### 步骤

在每台机器上执行：
```bash
thinbtd --tracker-port 8080 --p2p-port 16889 --ipc-port 16888 &
sleep 1

# 验证进程存活
ps aux | grep thinbtd

# 验证端口监听
ss -tlnp | grep -E "8080|16889|16888"

# IPC 连通性
tbt list

# 停止
kill $(pgrep thinbtd)
```

### 验收标准

4 台机器全部通过：进程存活、三个端口 listen、`tbt list` 返回正常 JSON。

---

## 任务 7c：Traker 连通性验证

### 拓扑

```
教师机 (192.168.177.56)
  └─ thinbtd (tracker:8080)

学生机
  └─ thinbtd → announce 到 192.168.177.56:8080
```

### 步骤

1. 教师机启动 thinbtd（自动启用内置 tracker）：
   ```bash
   thinbtd --tracker-port 8080 --p2p-port 16889 &
   ```

2. 学生机启动 thinbtd（配置 tracker 地址）：
   ```bash
   thinbtd --tracker-host 192.168.177.56 --tracker-port 8080 --p2p-port 16889 &
   ```

3. 学生机发起下载任务（触发 announce）：
   ```bash
   tbt add /tmp/UOS.iso.tseed /tmp/UOS_downloaded.iso
   ```

4. 检查 thinbtd 日志，确认：
   - TrackerClient 成功 connect 到 tracker
   - 收到 announce 响应
   - 获取 peer 列表

### 验收标准

Tracker announce 成功，日志中能看到 "announce ok, N peers" 或 peer 连接日志。

---

## 任务 7d：P2P 文件传输测试

### 场景：1 Seed + 1 Peer

```
教师机 (seed)                    学生机 1 (peer)
thinbtd --tracker-port 8080      thinbtd --tracker-host 192.168.177.56
tbt seed UOS.iso.tseed UOS.iso   tbt add UOS.iso.tseed /tmp/downloaded
```

### 步骤

1. 教师机做种
2. 学生机添加下载任务
3. 监控进度：`watch -n 2 'tbt list'`
4. 下载完成后，校验文件 SHA-256：
   ```bash
   sha256sum /tmp/downloaded
   sha256sum /path/to/original/UOS.iso
   ```
   两个 hash 必须一致。

### 验收标准

- 文件完整传输，SHA-256 一致
- 传输速度记录在日志中

---

## 任务 7e：多 Peer 并发测试

### 场景：1 Seed + 3 Peer

所有 3 台学生机同时从教师机下载 UOS.iso。

### 步骤

1. 教师机做种
2. 3 台学生机同时 `tbt add`
3. 监控所有机器的进度
4. 全部完成后校验 SHA-256

### 观察指标

| 指标 | 记录 |
|------|------|
| 教师机上行带宽 | iftop / nload |
| 每台学生机下载速度 | tbt update |
| 总完成时间 | 计时 |
| Choke/Unchoke 行为 | 日志中观察 10s choke tick |
| PEX gossip 是否生效 | 学生机之间是否有直接连接 |

### 验收标准

- 3 台全部完成，hash 一致
- 无 crash、无连接泄漏
- Choke 算法在日志中能看到 unchoke 分配（默认优化百兆上行，给最慢的 peer optimistic unchoke）

---

## 任务 7f：异常场景测试

### 7f-1：Peer 中途断开

1. 教师机做种，学生机下载中
2. 在学生机 `kill -9 $(pgrep thinbtd)`
3. 重新启动学生机 thinbtd，`tbt add` 同一任务
4. 验证断点续传（chunk 级别）

### 7f-2：Trackker 宕机降级

1. 学生机已获取 peer 列表后
2. 教师机 `kill $(pgrep thinbtd)`（tracker 宕机）
3. 验证学生机依靠 PEX gossip 继续交换 peer 信息

### 7f-3：多任务并行

1. 教师机同时做种 2 个文件
2. 不同学生机下载不同文件
3. 验证任务隔离（task_id 区分）

---

## 任务 7g：与旧 thinbt 对比

### 目标

新旧两个版本在相同条件下对比性能。

旧 thinbt 代码在 `/home/thinbt/`（C 语言版本），已编译。
新 thinbt 代码在 `/home/thinbt/new thinbt/`（C++17 重写）。

### 对比项

| 指标 | 旧 thinbt (C) | 新 thinbt (C++17) |
|------|:--:|:--:|
| 单文件做种速度 | ? MB/s | ? MB/s |
| 单 Peer 下载速度 | ? MB/s | ? MB/s |
| 3 Peer 并发下载速度 | ? MB/s | ? MB/s |
| 内存占用 | ? MB | ? MB |
| CPU 占用 | ? % | ? % |

### 步骤

1. 用旧 thinbt 跑一遍 7d 和 7e 的测试
2. 用新 thinbt 跑同样测试
3. 记录对比数据

---

## 任务 7h：稳定性验证（长时间运行）

### 目标

确认无内存泄漏、无连接泄漏、无 timer 堆积。

### 步骤

1. 教师机做种，保持 thinbtd 运行 1 小时
2. 学生机反复 add/remove 下载任务（10 次循环）
3. 监控进程内存：`watch -n 30 'ps -o rss,vsz -p $(pgrep thinbtd)'`
4. 检查文件描述符：`ls /proc/$(pgrep thinbtd)/fd | wc -l`

### 验收标准

- 内存无持续增长（允许 ±5MB 波动）
- fd 数量稳定（不持续增长）
- 无 crash

---

## 汇总输出

测试完成后，输出一份简短报告，包含：
1. 各测试通过/失败情况
2. 传输速度数据
3. 与旧 thinbt 的对比
4. 发现的问题（如有）
