#!/usr/bin/env bash
set -euo pipefail

PASS="${SSHPASS:-Admin@VOITerminal.OS}"
SEED_IP="192.168.177.56"
PEERS=("192.168.177.177" "192.168.177.150" "192.168.177.21")
ISO="/mnt/thinimg/uos-desktop-20-professional-W515x-W585x-1070-arm64-202509.iso"
TSEED="${ISO}.tseed"
REMOTE_DIR="/home/tc/new-thinbt/build_remote"
DAEMON="${REMOTE_DIR}/thinbtd"
CLI="${REMOTE_DIR}/tbt"
LOG="/tmp/thinbtd.log"

ipc_port_for() {
    case "$1" in
      192.168.177.56) echo 16888 ;;
      192.168.177.177) echo 17888 ;;
      192.168.177.150) echo 18888 ;;
      192.168.177.21) echo 19888 ;;
      *) echo 16888 ;;
    esac
}

p2p_port_for() {
    case "$1" in
      192.168.177.56) echo 16889 ;;
      192.168.177.177) echo 17889 ;;
      192.168.177.150) echo 18889 ;;
      192.168.177.21) echo 19889 ;;
      *) echo 16889 ;;
    esac
}

remote() {
    local ip="$1"
    shift
    SSHPASS="$PASS" sshpass -e ssh -o StrictHostKeyChecking=no "tc@${ip}" "$@"
}

seed_start() {
    local ipc_port p2p_port
    ipc_port="$(ipc_port_for "$SEED_IP")"
    p2p_port="$(p2p_port_for "$SEED_IP")"
    remote "$SEED_IP" "killall -9 thinbtd tbt >/dev/null 2>&1 || true; rm -f ${LOG}; nohup ${DAEMON} --ipc-port ${ipc_port} --p2p-port ${p2p_port} > ${LOG} 2>&1 </dev/null & sleep 2; ${CLI} --ipc-port ${ipc_port} seed ${TSEED} ${ISO}"
}

peer_start_daemon() {
    local ip="$1"
    local ipc_port p2p_port
    ipc_port="$(ipc_port_for "$ip")"
    p2p_port="$(p2p_port_for "$ip")"
    remote "$ip" "killall -9 thinbtd tbt >/dev/null 2>&1 || true; rm -f ${LOG}; nohup ${DAEMON} --ipc-port ${ipc_port} --p2p-port ${p2p_port} --tracker-host ${SEED_IP} > ${LOG} 2>&1 </dev/null & for i in 1 2 3 4 5 6 7 8 9 10; do ${CLI} --ipc-port ${ipc_port} list >/dev/null 2>&1 && exit 0; sleep 1; done; echo daemon_not_ready; tail -n 40 ${LOG}; exit 1"
}

peer_add() {
    local ip="$1"
    local out="$2"
    local ipc_port
    ipc_port="$(ipc_port_for "$ip")"
    remote "$ip" "rm -f ${out}; for i in 1 2 3 4 5 6; do ${CLI} --ipc-port ${ipc_port} list >/dev/null 2>&1 && break; sleep 1; done; ${CLI} --ipc-port ${ipc_port} add ${TSEED} ${out}"
}

sample_speed() {
    local ip="$1"
    local rounds="${2:-18}"
    local delay="${3:-5}"
    local ipc_port
    ipc_port="$(ipc_port_for "$ip")"
    remote "$ip" "python3 - <<'PY'
import json, subprocess, time
rounds = ${rounds}
delay = ${delay}
samples = []
for _ in range(rounds):
    raw = subprocess.check_output(['${CLI}', '--ipc-port', '${ipc_port}', 'list'], text=True).strip()
    try:
        obj = json.loads(raw)
        tasks = obj.get('data', {}).get('tasks', [])
        speed = tasks[0].get('speed_mib_s', 0.0) if tasks else 0.0
        progress = tasks[0].get('progress', 0.0) if tasks else 0.0
        state = tasks[0].get('state', '') if tasks else ''
    except Exception:
        speed = 0.0
        progress = 0.0
        state = 'parse_error'
    samples.append({'speed': speed, 'progress': progress, 'state': state})
    time.sleep(delay)

nonzero = [s['speed'] for s in samples if s['speed'] > 0]
tail = nonzero[-6:] if nonzero else []
avg = sum(tail)/len(tail) if tail else 0.0
mx = max(nonzero) if nonzero else 0.0
print(json.dumps({'samples': samples, 'nonzero_count': len(nonzero), 'tail_avg': avg, 'max': mx}))
PY"
}

collect_chunk_matrix() {
    local ip="$1"
    remote "$ip" "python3 - <<'PY'
import collections, json, re
counts = collections.Counter()
with open('${LOG}', 'r', errors='ignore') as f:
    for line in f:
        m = re.search(r'\\[chunk_done\\].*from=([0-9.]+)', line)
        if m:
            counts[m.group(1)] += 1
print(json.dumps(counts, sort_keys=True))
PY"
}

disk_probe() {
    local ip="$1"
    remote "$ip" "python3 - <<'PY'
import json, subprocess
cmd = \"dd if=/dev/zero of=/mnt/thinimg/.thinbt-dd-test bs=4M count=64 conv=fdatasync 2>&1 && sync && dd if=/mnt/thinimg/.thinbt-dd-test of=/dev/null bs=4M 2>&1 && rm -f /mnt/thinimg/.thinbt-dd-test\"
out = subprocess.check_output(['sh', '-lc', cmd], text=True, stderr=subprocess.STDOUT)
print(json.dumps({'output': out}))
PY"
}

case "${1:-}" in
  seed-start) seed_start ;;
  peer-start-daemon) peer_start_daemon "$2" ;;
  peer-add) peer_add "$2" "$3" ;;
  sample-speed) sample_speed "$2" "${3:-18}" "${4:-5}" ;;
  collect-chunks) collect_chunk_matrix "$2" ;;
  disk-probe) disk_probe "$2" ;;
  *)
    echo "usage: $0 {seed-start|peer-start-daemon <ip>|peer-add <ip> <out>|sample-speed <ip> [rounds] [delay]|collect-chunks <ip>|disk-probe <ip>}" >&2
    exit 1
    ;;
esac
