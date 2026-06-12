#!/usr/bin/env python3
"""thinBT 远程测试辅助脚本 — SSH 批量管理 4 台机器"""
import paramiko
import sys
import time
import os
import threading

HOSTS = {
    "74":  "192.168.177.74",
    "177": "192.168.177.177",
    "41":  "192.168.177.41",
    "56":  "192.168.177.56",
}

PEERS = ["74", "177", "41"]  # download nodes
SEED = "56"

USER = "tc"
PASSWORD = "Admin@VOITerminal.OS"
PORT = 22

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
TARBALL = os.path.join(SCRIPT_DIR, "thinbt-src.tar.gz")
REMOTE_DIR = "/home/tc/new-thinbt"
BIN_DIR = "/home/tc/bin"
TEST_ISO = "/mnt/thinimg/uos-desktop-20-professional-W515x-W585x-1070-arm64-202509.iso"
TEST_TSEED = "/mnt/thinimg/uos-desktop-20-professional-W515x-W585x-1070-arm64-202509.iso.tseed"
DOWNLOAD_PATH = "/mnt/thinimg/test_download.iso"

# ── SSH helpers ─────────────────────────────────────────────

def ssh_exec(host_ip, cmd, timeout=60, get_pty=False):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(host_ip, port=PORT, username=USER, password=PASSWORD,
                       timeout=10, allow_agent=False, look_for_keys=False)
        stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout, get_pty=get_pty)
        out = stdout.read().decode('utf-8', errors='replace')
        err = stderr.read().decode('utf-8', errors='replace')
        exit_code = stdout.channel.recv_exit_status()
        return out, err, exit_code
    except Exception as e:
        return "", str(e), -1
    finally:
        client.close()

def scp_put(host_ip, local_path, remote_path):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(host_ip, port=PORT, username=USER, password=PASSWORD,
                       timeout=10, allow_agent=False, look_for_keys=False)
        sftp = client.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.chmod(remote_path, 0o755)
        sftp.close()
        return True, ""
    except Exception as e:
        return False, str(e)
    finally:
        client.close()

def run_on_all(cmd, label="", timeout=60):
    print(f"\n{'='*60}")
    print(f"[{label or cmd[:50]}]")
    print(f"{'='*60}")
    for name, ip in HOSTS.items():
        out, err, ec = ssh_exec(ip, cmd, timeout=timeout)
        status = "OK" if ec == 0 else f"EXIT({ec})"
        print(f"\n--- {name} ({ip}) [{status}] ---")
        if out: print(out.rstrip())
        if err: print(f"[STDERR]: {err.rstrip()}")

def run_on_one(name_or_ip, cmd, timeout=60):
    ip = HOSTS.get(name_or_ip, name_or_ip)
    out, err, ec = ssh_exec(ip, cmd, timeout=timeout)
    print(f"--- {name_or_ip} ({ip}) [EXIT:{ec}] ---")
    if out: print(out.rstrip())
    if err: print(f"[STDERR]: {err.rstrip()}")
    return out, err, ec

def run_on_peers(cmd, label="", timeout=60):
    print(f"\n{'='*60}")
    print(f"[{label or cmd[:50]}]")
    print(f"{'='*60}")
    for name in PEERS:
        ip = HOSTS[name]
        out, err, ec = ssh_exec(ip, cmd, timeout=timeout)
        status = "OK" if ec == 0 else f"EXIT({ec})"
        print(f"\n--- {name} ({ip}) [{status}] ---")
        if out: print(out.rstrip())
        if err: print(f"[STDERR]: {err.rstrip()}")

# ── Workflow steps ──────────────────────────────────────────

def step_upload_sources():
    """上传源码到所有机器 (需要先手动创建 thinbt-src.tar.gz)"""
    print("\n" + "="*60)
    print("[STEP 1] 上传源码")
    print("="*60)

    if not os.path.exists(TARBALL):
        print(f"错误: 找不到 {TARBALL}")
        print("请先运行: cd /d/projects/new-thinbt && tar czf Testing/thinbt-src.tar.gz --exclude=build* --exclude=Testing --exclude=.git --exclude=.claude --exclude=.codex src/ third_party/ CMakeLists.txt")
        return False

    size_mb = os.path.getsize(TARBALL) / 1024 / 1024
    print(f"源码包: {TARBALL} ({size_mb:.1f} MB)")

    for name, ip in HOSTS.items():
        print(f"  上传到 {name} ({ip})...", end=" ", flush=True)
        ok, err = scp_put(ip, TARBALL, f"{REMOTE_DIR}.tar.gz")
        if not ok:
            print(f"FAIL: {err}")
            return False
        print("OK")
        # Extract
        out, err, ec = ssh_exec(ip, f"mkdir -p {REMOTE_DIR} && cd {REMOTE_DIR} && tar xzf {REMOTE_DIR}.tar.gz --strip-components=0 2>&1 && echo 'extracted'")
        if ec != 0:
            print(f"  解压失败: {err}")
            return False
        print(f"  解压完成")

    return True

def step_build():
    """在 56 上编译，然后分发到其他机器"""
    print("\n" + "="*60)
    print("[STEP 2] 编译 thinbtd + tbt")
    print("="*60)

    ip = HOSTS[SEED]
    # yyjson is C, must compile separately with gcc
    src_dir = f"{REMOTE_DIR}/src"
    tp_dir = f"{REMOTE_DIR}/third_party"
    common_cpp = " ".join([
        f"{src_dir}/common/hash.cpp",
        f"{src_dir}/common/file_util.cpp",
        f"{src_dir}/common/net_util.cpp",
    ])
    daemon_cpp = " ".join([
        f"{src_dir}/daemon/chunk_assembler.cpp",
        f"{src_dir}/daemon/io_worker.cpp",
        f"{src_dir}/daemon/verify_worker.cpp",
        f"{src_dir}/daemon/segment_io.cpp",
        f"{src_dir}/daemon/scheduler.cpp",
        f"{src_dir}/daemon/protocol.cpp",
        f"{src_dir}/daemon/peer_session.cpp",
        f"{src_dir}/daemon/sendfile_pool.cpp",
        f"{src_dir}/daemon/file_read_pool.cpp",
        f"{src_dir}/daemon/peer_manager.cpp",
        f"{src_dir}/daemon/task_manager.cpp",
        f"{src_dir}/daemon/ipc_server.cpp",
        f"{src_dir}/daemon/tracker_client.cpp",
        f"{src_dir}/daemon/tracker_server.cpp",
        f"{src_dir}/daemon/tracker_acceptor.cpp",
    ])
    cdc_cpp = f"{src_dir}/cdc/fastcdc.cpp"
    seed_cpp = f"{src_dir}/seed/seed_reader.cpp {src_dir}/seed/seed_writer.cpp"
    cli_cpp = f"{src_dir}/cli/tbt.cpp {src_dir}/cli/cli_commands.cpp"

    build_script = f"""#!/bin/sh
set -e
cd {REMOTE_DIR}
rm -rf build-target
mkdir -p build-target
cd build-target

CFLAGS="-std=c99 -O3 -DNDEBUG"
CXXFLAGS="-std=c++17 -O3 -DNDEBUG"
INCLUDES="-I{src_dir} -I{tp_dir}/asio/asio/include -I{tp_dir}/moodycamel -I{tp_dir}/yyjson"
LIBS="-lssl -lcrypto -lpthread"

echo "=== Compiling yyjson (C) ==="
gcc $CFLAGS $INCLUDES -c {tp_dir}/yyjson/yyjson.c -o yyjson.o

echo "=== Compiling common (C++) ==="
for f in {common_cpp}; do echo "  $f"; g++ $CXXFLAGS $INCLUDES -c "$f"; done

echo "=== Compiling cdc (C++) ==="
for f in {cdc_cpp}; do echo "  $f"; g++ $CXXFLAGS $INCLUDES -c "$f"; done

echo "=== Compiling seed (C++) ==="
for f in {seed_cpp}; do echo "  $f"; g++ $CXXFLAGS $INCLUDES -c "$f"; done

echo "=== Compiling daemon (C++) ==="
for f in {daemon_cpp}; do echo "  $f"; g++ $CXXFLAGS $INCLUDES -c "$f"; done

echo "=== Compiling main.cpp ==="
g++ $CXXFLAGS $INCLUDES -c {src_dir}/daemon/main.cpp -o main.o

echo "=== Linking thinbtd ==="
g++ $CXXFLAGS yyjson.o hash.o file_util.o net_util.o fastcdc.o seed_reader.o seed_writer.o chunk_assembler.o io_worker.o verify_worker.o segment_io.o scheduler.o protocol.o peer_session.o sendfile_pool.o file_read_pool.o peer_manager.o task_manager.o ipc_server.o tracker_client.o tracker_server.o tracker_acceptor.o main.o $LIBS -o thinbtd

echo "=== Compiling cli (C++) ==="
for f in {cli_cpp}; do echo "  $f"; g++ $CXXFLAGS $INCLUDES -c "$f"; done

echo "=== Linking tbt ==="
g++ $CXXFLAGS yyjson.o hash.o file_util.o net_util.o fastcdc.o seed_reader.o seed_writer.o tbt.o cli_commands.o $LIBS -o tbt

echo "=== Strip binaries ==="
strip thinbtd tbt 2>/dev/null || true
echo "=== Done ==="
ls -lh thinbtd tbt
"""
    # Write build script to remote and execute
    script_remote = f"{REMOTE_DIR}/build.sh"
    out, err, ec = ssh_exec(ip, f"cat > {script_remote} << 'BUILD_EOF'\n{build_script}\nBUILD_EOF\nchmod +x {script_remote}")
    if ec != 0:
        print(f"写入编译脚本失败: {err}")
        return False

    print(f"开始在 {SEED} ({ip}) 上编译（预计 1-3 分钟）...")
    out, err, ec = ssh_exec(ip, f"sh {script_remote}", timeout=300)
    print(out)
    if err:
        print(f"[STDERR]: {err}")
    if ec != 0:
        print(f"编译失败! exit={ec}")
        return False

    # Copy binaries to bin dir
    ssh_exec(ip, f"mkdir -p {BIN_DIR} && cp {REMOTE_DIR}/build-target/thinbtd {BIN_DIR}/ && cp {REMOTE_DIR}/build-target/tbt {BIN_DIR}/")

    # Copy from 56 to other machines
    print("\n分发二进制到其他机器...")
    for name in PEERS:
        peer_ip = HOSTS[name]
        print(f"  分发到 {name} ({peer_ip})...", end=" ", flush=True)
        # Use scp from 56 to peer (SSH keyless, use password)
        ok, err = scp_put(peer_ip, "/tmp/thinbtd_dummy", "/dev/null")  # test conn
        # Actually download from 56 then upload
        # First download from 56 to local temp
        local_thinbtd = "/tmp/thinbtd_tmp"
        local_tbt = "/tmp/tbt_tmp"
        # We can't directly SCP between remote machines easily with paramiko
        # Instead: download from 56 to local, then upload to each peer
        pass

    # Download from 56 to local, then upload to peers
    print("\n下载编译产物到本地...")
    local_thinbtd = os.path.join(SCRIPT_DIR, "thinbtd_tmp")
    local_tbt = os.path.join(SCRIPT_DIR, "tbt_tmp")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, port=PORT, username=USER, password=PASSWORD,
                   timeout=10, allow_agent=False, look_for_keys=False)
    sftp = client.open_sftp()
    sftp.get(f"{BIN_DIR}/thinbtd", local_thinbtd)
    sftp.get(f"{BIN_DIR}/tbt", local_tbt)
    sftp.close()
    client.close()
    print(f"  thinbtd: {os.path.getsize(local_thinbtd)/1024:.1f} MB")
    print(f"  tbt: {os.path.getsize(local_tbt)/1024:.1f} MB")

    for name in PEERS:
        peer_ip = HOSTS[name]
        ssh_exec(peer_ip, f"mkdir -p {BIN_DIR}")
        ok1, _ = scp_put(peer_ip, local_thinbtd, f"{BIN_DIR}/thinbtd")
        ok2, _ = scp_put(peer_ip, local_tbt, f"{BIN_DIR}/tbt")
        if ok1 and ok2:
            print(f"  ✓ {name} ({peer_ip})")
        else:
            print(f"  ✗ {name} ({peer_ip})")

    # Verify
    run_on_all(f"ls -lh {BIN_DIR}/thinbtd {BIN_DIR}/tbt 2>&1", "验证二进制部署")
    return True

def step_generate_tseed():
    """在 56 上生成种子文件"""
    print("\n" + "="*60)
    print("[STEP 3] 生成 .tseed 种子文件")
    print("="*60)

    ip = HOSTS[SEED]

    # Check if tseed already exists
    out, _, ec = ssh_exec(ip, f"ls -lh {TEST_TSEED} 2>/dev/null")
    if ec == 0:
        print(f"种子文件已存在: {out.strip()}")
        # Check if we should regenerate
        out2, _, _ = ssh_exec(ip, f"{BIN_DIR}/tbt info {TEST_TSEED} 2>&1")
        print(f"种子信息:\n{out2}")
        return True

    print(f"生成种子: {TEST_ISO} -> {TEST_TSEED}")
    out, err, ec = ssh_exec(ip, f"{BIN_DIR}/tbt make {TEST_ISO} --output {TEST_TSEED} 2>&1", timeout=120)
    print(out)
    if err: print(f"[STDERR]: {err}")
    if ec != 0:
        print(f"种子生成失败! exit={ec}")
        return False

    # Distribute tseed to peers
    print("\n分发种子到其他机器...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, port=PORT, username=USER, password=PASSWORD,
                   timeout=10, allow_agent=False, look_for_keys=False)
    sftp = client.open_sftp()
    sftp.get(TEST_TSEED, "/tmp/test.tseed")
    sftp.close()
    client.close()

    for name in PEERS:
        peer_ip = HOSTS[name]
        ok, err = scp_put(peer_ip, "/tmp/test.tseed", TEST_TSEED)
        print(f"  {name}: {'OK' if ok else 'FAIL - '+err}")

    return True

def step_kill_all():
    """停止所有 thinbtd 进程"""
    print("\n停止已有 thinbtd 进程...")
    run_on_all("pkill -9 thinbtd 2>/dev/null; sleep 1; echo 'done'", "停止 thinbtd")
    time.sleep(2)

def step_start_seed():
    """在 56 上启动 tracker + seed"""
    print("\n" + "="*60)
    print("[STEP 4] 启动 Tracker + Seed (56)")
    print("="*60)

    ip = HOSTS[SEED]

    # Start thinbtd in background (tracker mode since no --tracker-host)
    cmd = f"nohup {BIN_DIR}/thinbtd --tracker-port 8080 --p2p-port 16889 > /tmp/thinbtd_seed.log 2>&1 &"
    out, err, ec = ssh_exec(ip, cmd)
    time.sleep(2)

    # Verify it's running
    out, err, ec = ssh_exec(ip, "ps aux | grep thinbtd | grep -v grep")
    if ec != 0 or not out.strip():
        print("thinbtd 启动失败!")
        # Show log
        log, _, _ = ssh_exec(ip, "tail -20 /tmp/thinbtd_seed.log")
        print(f"日志:\n{log}")
        return False
    print(f"thinbtd 已启动:\n{out}")

    # Verify ports
    out, _, _ = ssh_exec(ip, "netstat -tlnp 2>/dev/null | grep -E '8080|16889|16888' || ss -tlnp | grep -E '8080|16889|16888'")
    print(f"监听端口:\n{out}")

    # Start seeding
    print(f"\n启动做种: {TEST_TSEED} -> {TEST_ISO}")
    out, err, ec = ssh_exec(ip, f"{BIN_DIR}/tbt seed {TEST_TSEED} {TEST_ISO} 2>&1")
    print(out)
    if err: print(f"[STDERR]: {err}")
    if ec != 0:
        print(f"做种启动失败!")
        return False

    return True

def step_start_peers():
    """在其他 3 台机器上启动 thinbtd 并开始下载"""
    print("\n" + "="*60)
    print("[STEP 5] 启动 Peer 下载")
    print("="*60)

    tracker_host = HOSTS[SEED]
    ok_count = 0
    for name in PEERS:
        ip = HOSTS[name]
        print(f"\n--- {name} ({ip}) ---")

        # Remove old download file if exists
        ssh_exec(ip, f"rm -f {DOWNLOAD_PATH}")

        # Kill existing thinbtd
        ssh_exec(ip, "pkill -9 thinbtd 2>/dev/null; sleep 1")

        # Start thinbtd with tracker
        out, err, ec = ssh_exec(ip,
            f"nohup {BIN_DIR}/thinbtd --tracker-host {tracker_host} --tracker-port 8080 --p2p-port 16889 > /tmp/thinbtd_peer.log 2>&1 &")
        time.sleep(2)

        # Verify
        out, err, ec = ssh_exec(ip, "ps aux | grep thinbtd | grep -v grep")
        if ec != 0 or not out.strip():
            print(f"  thinbtd 启动失败!")
            log, _, _ = ssh_exec(ip, "tail -10 /tmp/thinbtd_peer.log")
            print(f"  日志: {log}")
            continue

        # Start download
        print(f"  开始下载...")
        out, err, ec = ssh_exec(ip, f"{BIN_DIR}/tbt add {TEST_TSEED} {DOWNLOAD_PATH} 2>&1")
        print(f"  {out.strip()}")
        if err: print(f"  [STDERR]: {err}")
        if ec == 0:
            ok_count += 1
        else:
            print(f"  添加下载任务失败!")

    print(f"\n成功启动 {ok_count}/{len(PEERS)} 个 Peer 下载")

def step_status():
    """查看所有机器状态"""
    print("\n" + "="*60)
    print("[STATUS] 所有机器任务状态")
    print("="*60)
    for name, ip in HOSTS.items():
        out, err, ec = ssh_exec(ip, f"{BIN_DIR}/tbt list 2>&1")
        status = "OK" if ec == 0 else f"EXIT({ec})"
        print(f"\n--- {name} ({ip}) [{status}] ---")
        if out: print(out.strip())
        if err: print(f"[STDERR]: {err.strip()}")

def step_logs():
    """查看 thinbtd 日志"""
    print("\n" + "="*60)
    print("[LOGS] thinbtd 日志 (最近 30 行)")
    print("="*60)
    for name, ip in HOSTS.items():
        logfile = "/tmp/thinbtd_seed.log" if name == SEED else "/tmp/thinbtd_peer.log"
        out, _, _ = ssh_exec(ip, f"tail -30 {logfile} 2>/dev/null")
        print(f"\n--- {name} ({ip}) ---")
        if out: print(out.strip())

def step_verify():
    """验证下载文件 SHA-256 是否与源文件一致"""
    print("\n" + "="*60)
    print("[VERIFY] SHA-256 校验")
    print("="*60)

    # Get source hash from 56
    out, _, _ = ssh_exec(HOSTS[SEED], f"sha256sum {TEST_ISO} 2>/dev/null | awk '{{print $1}}'")
    source_hash = out.strip()
    print(f"源文件 ({SEED}): {source_hash}")

    all_ok = True
    for name in PEERS:
        ip = HOSTS[name]
        out, _, ec = ssh_exec(ip, f"sha256sum {DOWNLOAD_PATH} 2>/dev/null | awk '{{print $1}}'")
        dl_hash = out.strip()
        if not dl_hash:
            print(f"  {name}: 文件不存在或无法读取")
            all_ok = False
            continue
        match = "✓ MATCH" if dl_hash == source_hash else "✗ MISMATCH"
        print(f"  {name}: {dl_hash} {match}")
        if dl_hash != source_hash:
            all_ok = False
            # Check file size
            out2, _, _ = ssh_exec(ip, f"ls -lh {DOWNLOAD_PATH} 2>/dev/null")
            print(f"         文件信息: {out2.strip()}")

    if all_ok:
        print("\n✓ 所有文件校验通过!")
    else:
        print("\n✗ 存在校验失败的文件")
    return all_ok

def step_monitor(interval=5, count=12):
    """持续监控进度"""
    print(f"\n{'='*60}")
    print(f"[MONITOR] 每 {interval}s 刷新，共 {count} 次")
    print(f"{'='*60}")

    for i in range(count):
        print(f"\n{'─'*50}")
        print(f"[{time.strftime('%H:%M:%S')}] 第 {i+1}/{count} 次")
        for name, ip in HOSTS.items():
            out, _, ec = ssh_exec(ip, f"{BIN_DIR}/tbt list 2>&1", timeout=10)
            if out.strip():
                # Try to extract progress info
                for line in out.strip().split('\n'):
                    if 'progress' in line.lower() or 'speed' in line.lower() or 'state' in line.lower() or 'bytes' in line.lower():
                        print(f"  {name}: {line.strip()}")
                # If no structured output, just print first line
                if ec == 0 and out.strip():
                    print(f"  {name}: {out.strip()[:120]}")
            else:
                print(f"  {name}: (无输出)")
        time.sleep(interval)

def step_clean():
    """清理所有机器的测试文件"""
    run_on_all("pkill -9 thinbtd 2>/dev/null; sleep 1; echo 'stopped'", "停止 thinbtd")
    run_on_all(f"rm -f {DOWNLOAD_PATH} /tmp/thinbtd_*.log {REMOTE_DIR}.tar.gz; echo 'cleaned'", "清理下载和日志")

# ── Main ────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python remote.py check           — 检查环境")
        print("  python remote.py upload          — 上传源码")
        print("  python remote.py build           — 编译 + 分发二进制")
        print("  python remote.py tseed           — 生成种子")
        print("  python remote.py kill            — 停止所有 thinbtd")
        print("  python remote.py seed            — 启动 tracker + 做种")
        print("  python remote.py peers           — 启动 3 台 Peer 下载")
        print("  python remote.py status          — 查看任务状态")
        print("  python remote.py logs            — 查看日志")
        print("  python remote.py monitor         — 持续监控进度")
        print("  python remote.py verify          — SHA-256 校验")
        print("  python remote.py clean           — 清理")
        print("")
        print("  一键流程:")
        print("  python remote.py full            — upload + build + tseed + seed + peers")
        print("  python remote.py test            — kill + seed + peers + monitor + verify")
        sys.exit(0)

    cmd = sys.argv[1]

    actions = {
        "check":  lambda: run_on_all("uname -a && free -h && ls -lh /mnt/thinimg/*.iso 2>/dev/null", "环境检查"),
        "upload": step_upload_sources,
        "build":  step_build,
        "tseed":  step_generate_tseed,
        "kill":   step_kill_all,
        "seed":   step_start_seed,
        "peers":  step_start_peers,
        "status": step_status,
        "logs":   step_logs,
        "monitor": lambda: step_monitor(5, 20),
        "verify": step_verify,
        "clean":  step_clean,
        "run":    lambda: run_on_all(sys.argv[2]),
        "one":    lambda: run_on_one(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ""),
    }

    if cmd == "full":
        for step in [step_kill_all, step_upload_sources, step_build, step_generate_tseed, step_kill_all, step_start_seed, step_start_peers]:
            if not step():
                print(f"\n✗ 步骤失败，停止执行")
                sys.exit(1)
        step_status()
        step_monitor(5, 24)  # monitor for 2 minutes
        step_verify()
    elif cmd == "test":
        for step in [step_kill_all, step_start_seed, step_start_peers]:
            if not step():
                print(f"\n✗ 步骤失败，停止执行")
                sys.exit(1)
        step_status()
        step_monitor(5, 24)
        step_verify()
    elif cmd in actions:
        actions[cmd]()
    else:
        print(f"未知命令: {cmd}")
        sys.exit(1)

if __name__ == "__main__":
    main()
