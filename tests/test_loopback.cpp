#include "daemon/peer_manager.hpp"
#include "daemon/peer_session.hpp"
#include "daemon/scheduler.hpp"
#include "daemon/chunk_assembler.hpp"
#include "daemon/io_worker.hpp"
#include "daemon/segment_io.hpp"
#include "daemon/protocol.hpp"
#include "seed/seed_reader.hpp"
#include "seed/tseed.hpp"
#include "common/hash.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include <atomic>

using namespace thinbt;

static constexpr size_t   FILE_SIZE    = 512 * 1024;
static constexpr uint32_t CHUNK_SIZE   = 128 * 1024;
static constexpr uint32_t NUM_CHUNKS   = FILE_SIZE / CHUNK_SIZE;

static const char* TEST_FILE      = "/tmp/thinbt_loopback_test_source.bin";
static const char* TSEED_PATH     = "/tmp/thinbt_loopback_test.tseed";
static const char* DOWNLOAD_PATH  = "/tmp/thinbt_loopback_test_download.bin";

static void create_test_file() {
    std::ofstream f(TEST_FILE, std::ios::binary);
    for (size_t i = 0; i < FILE_SIZE; i++)
        f.put(static_cast<char>((i * 7 + 13) % 256));
}

namespace thinbt {
void write_tseed(const std::string& output_path,
                 const std::string& file_path,
                 const std::string& file_name,
                 const std::string& announce_url,
                 const std::vector<ChunkEntry>& chunks,
                 uint32_t min_chunk_size, uint32_t avg_chunk_size, uint32_t max_chunk_size);
}

static std::vector<uint64_t> create_tseed() {
    std::vector<ChunkEntry> chunks;
    for (uint32_t i = 0; i < NUM_CHUNKS; i++) {
        ChunkEntry ce{};
        ce.offset = static_cast<uint64_t>(i) * CHUNK_SIZE;
        ce.length = CHUNK_SIZE;
        memset(ce.sha256, 0, 32);
        chunks.push_back(ce);
    }
    write_tseed(TSEED_PATH, TEST_FILE, "test_vm.qcow2",
                "thinbt://127.0.0.1:8080/announce",
                chunks, 16384, 131072, 1048576);
    std::vector<uint64_t> offsets;
    for (const auto& c : chunks) offsets.push_back(c.offset);
    return offsets;
}

int main() {
    std::cout << "=== test_loopback ===" << std::endl;

    // ── 准备 ──
    create_test_file();
    auto chunk_offsets = create_tseed();
    auto seed_file = read_tseed(TSEED_PATH);
    uint32_t chunk_count = seed_file->header.chunk_count;
    Sha1Digest info_hash = seed_file->info_hash;

    std::cout << "  file=" << FILE_SIZE << " bytes, chunks=" << chunk_count << std::endl;
    std::cout << "  info_hash=" << sha1_hex(info_hash) << std::endl;

    asio::io_context io;
    const uint32_t local_speed = 1000;

    // ═══════════════════════════════════════════════════════════
    // Seed (A): p2p=16889
    // ═══════════════════════════════════════════════════════════
    std::cout << "\n[Seed] port 16889" << std::endl;

    int seed_fd = ::open(TEST_FILE, O_RDONLY);
    if (seed_fd < 0) { std::cerr << "FAIL: open test file" << std::endl; return 1; }

    Scheduler seed_sched;
    seed_sched.init(chunk_count, local_speed,
        [](uint32_t, uint32_t, uint32_t, uint32_t) {},
        [](uint32_t) {});
    std::vector<bool> seed_bf(chunk_count, true);
    seed_sched.mark_all_complete(seed_bf);

    auto seed_asm = std::make_unique<ChunkAssembler[]>(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++) seed_asm[i].init(nullptr, CHUNK_SIZE);
    IOWorkerPool seed_io;
    seed_io.start(1, seed_asm.get(), [](ChunkCompleteMsg) {});

    PeerManager seed_pm(io, seed_sched, &seed_io, info_hash, local_speed, 16889);
    seed_pm.set_file_fd(seed_fd);
    seed_pm.set_initial_bitfield(seed_bf);
    seed_pm.set_chunk_offsets(&chunk_offsets);
    seed_pm.start_accept();

    // ═══════════════════════════════════════════════════════════
    // Peer (B): p2p=16890
    // ═══════════════════════════════════════════════════════════
    std::cout << "[Peer] port 16890" << std::endl;

    SegmentWriter peer_writer;
    if (!peer_writer.open(DOWNLOAD_PATH, FILE_SIZE)) { std::cerr << "FAIL: peer_writer.open" << std::endl; return 1; }

    auto peer_asm = std::make_unique<ChunkAssembler[]>(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++) {
        uint8_t* base = peer_writer.get_chunk_base(
            static_cast<uint64_t>(i) * CHUNK_SIZE, CHUNK_SIZE);
        if (!base) { std::cerr << "FAIL: get_chunk_base null for chunk " << i << std::endl; return 1; }
        peer_asm[i].init(base, CHUNK_SIZE);
    }

    std::vector<ChunkCompleteMsg> peer_completions;
    IOWorkerPool peer_io;
    peer_io.start(2, peer_asm.get(),
        [&](ChunkCompleteMsg msg) { peer_completions.push_back(msg); });

    Scheduler peer_sched;
    peer_sched.init(chunk_count, local_speed,
        [](uint32_t, uint32_t, uint32_t, uint32_t) {},
        [](uint32_t) {});

    PeerManager peer_pm(io, peer_sched, &peer_io, info_hash, local_speed, 16890);
    peer_pm.set_file_fd(peer_writer.get_file_fd());
    std::vector<bool> peer_bf(chunk_count, false);
    peer_pm.set_initial_bitfield(peer_bf);
    peer_pm.set_chunk_offsets(&chunk_offsets);
    peer_pm.start_accept();

    // ═══════════════════════════════════════════════════════════
    // 连接 + 握手
    // ═══════════════════════════════════════════════════════════
    std::cout << "\n[Test] Connecting..." << std::endl;
    peer_pm.connect_to("127.0.0.1", 16889, 0x02);

    // poll 驱动握手 + BITFIELD 交换（所有 bug 已修复，poll 应正常工作）
    for (int i = 0; i < 200; i++) {
        io.poll();
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }

    // Unchoke
    seed_pm.tick_choke();
    for (int i = 0; i < 50; i++) {
        io.poll();
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }

    // 检查握手是否完成
    bool connected = false;
    for (const auto& sess : peer_pm.sessions()) {
        if (sess->remote_bitfield().size() > 0 && !sess->is_choked()) {
            connected = true;
        }
    }
    std::cout << "  connected=" << connected
              << " seed_peers=" << seed_pm.peer_count()
              << " peer_peers=" << peer_pm.peer_count() << std::endl;

    if (!connected) {
        std::cerr << "FAIL: Handshake incomplete." << std::endl;
        return 1;
    }

    // ═══════════════════════════════════════════════════════════
    // 传输循环
    // ═══════════════════════════════════════════════════════════
    std::cout << "[Test] Transferring..." << std::endl;
    int iteration = 0, total_reqs = 0;

    while (peer_sched.phase() != SchedulerPhase::DONE && iteration < 1000) {
        io.poll();

        // 处理 chunk 完成
        peer_sched.process_completions(peer_completions);

        // 每 2 次迭代发送 REQUEST
        if (iteration % 2 == 0) {
            for (uint32_t ci = 0; ci < chunk_count; ci++) {
                if (peer_asm[ci].is_complete()) continue;
                for (const auto& sess : peer_pm.sessions()) {
                    const auto& rf = sess->remote_bitfield();
                    if (ci >= rf.size() || !rf[ci]) continue;
                    if (sess->is_choked()) continue;
                    const auto& subs = peer_asm[ci].sub_blocks();
                    for (const auto& sb : subs) {
                        if (sess->pending_requests() >= sess->pipeline_cap()) break;
                        sess->send_message(build_request(ci, sb.begin, sb.length));
                        sess->inc_pending();
                        total_reqs++;
                    }
                    break;
                }
            }
        }

        if (iteration % 100 == 0) {
            std::cout << "  [" << iteration << "] done="
                      << (chunk_count - peer_sched.missing_count())
                      << "/" << chunk_count << " reqs=" << total_reqs << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(200));
        iteration++;
    }

    // 最后刷新
    for (int i = 0; i < 100; i++) {
        io.poll();
        peer_sched.process_completions(peer_completions);
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    // ═══════════════════════════════════════════════════════════
    // 验证
    // ═══════════════════════════════════════════════════════════
    std::cout << "\n[Verify] phase="
              << (peer_sched.phase() == SchedulerPhase::DONE ? "DONE" : "NOT_DONE")
              << " missing=" << peer_sched.missing_count()
              << " reqs=" << total_reqs << std::endl;

    if (peer_sched.phase() != SchedulerPhase::DONE) {
        std::cerr << "FAIL: incomplete" << std::endl;
        for (uint32_t ci = 0; ci < chunk_count; ci++)
            if (!peer_asm[ci].is_complete())
                std::cerr << "  chunk " << ci << " pending="
                          << peer_asm[ci].pending_count_val() << std::endl;
        return 1;
    }

    peer_io.stop();
    seed_io.stop();
    peer_writer.close();

    auto orig_sha = sha256_file(TEST_FILE);
    auto dl_sha = sha256_file(DOWNLOAD_PATH);
    if (orig_sha != dl_sha) {
        std::cerr << "FAIL: SHA-256 mismatch\n  orig=" << sha256_hex(orig_sha)
                  << "\n  dl=" << sha256_hex(dl_sha) << std::endl;
        return 1;
    }
    std::cout << "  SHA-256 OK: " << sha256_hex(dl_sha) << std::endl;

    std::ifstream df(DOWNLOAD_PATH, std::ios::binary | std::ios::ate);
    if (static_cast<size_t>(df.tellg()) != FILE_SIZE) { std::cerr << "FAIL: file size mismatch" << std::endl; return 1; }

    ::close(seed_fd);
    std::remove(TEST_FILE);
    std::remove(TSEED_PATH);
    std::remove(DOWNLOAD_PATH);

    std::cout << "\n[PASS] End-to-end P2P transfer OK!" << std::endl;
    return 0;
}
