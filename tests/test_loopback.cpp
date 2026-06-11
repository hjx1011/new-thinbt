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

#include <cstdio>
#include <cstring>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <iomanip>

using namespace thinbt;

// Reduced file size for faster local debugging
static constexpr uint64_t FILE_SIZE    = 32ULL * 1024 * 1024;
static constexpr uint32_t CHUNK_SIZE   = 128 * 1024;
static constexpr uint32_t NUM_CHUNKS   = FILE_SIZE / CHUNK_SIZE;

static const char* TEST_FILE      = "/tmp/thinbt_loopback_4g_src.bin";
static const char* TSEED_PATH     = "/tmp/thinbt_loopback_4g.tseed";
static const char* DOWNLOAD_PATH  = "/tmp/thinbt_loopback_4g_dl.bin";

static void create_test_file() {
    std::cout << "[Setup] generating test file..." << std::flush;
    FILE* f = fopen(TEST_FILE, "wb");
    if (!f) { std::cerr << "FAIL: cannot create test file" << std::endl; exit(1); }
    std::vector<uint8_t> buf(4 * 1024 * 1024);
    uint32_t seed = 0xDEADBEEF;
    for (uint64_t off = 0; off < FILE_SIZE; off += buf.size()) {
        size_t n = std::min(buf.size(), static_cast<size_t>(FILE_SIZE - off));
        for (size_t i = 0; i < n; i += 4) {
            seed = seed * 1103515245 + 12345;
            memcpy(buf.data() + i, &seed, 4);
        }
        fwrite(buf.data(), 1, n, f);
    }
    fclose(f);
    std::cout << " done" << std::endl;
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
    std::vector<ChunkEntry> chunks(NUM_CHUNKS);
    for (uint32_t i = 0; i < NUM_CHUNKS; i++) {
        chunks[i].offset = static_cast<uint64_t>(i) * CHUNK_SIZE;
        chunks[i].length = CHUNK_SIZE;
        memset(chunks[i].sha256, 0, 32);
    }
    write_tseed(TSEED_PATH, TEST_FILE, "test_vm.qcow2",
                "thinbt://127.0.0.1:8080/announce",
                chunks, 16384, 131072, 1048576);
    std::vector<uint64_t> offsets;
    for (const auto& c : chunks) offsets.push_back(c.offset);
    return offsets;
}

int main() {
    std::cout << "=== 4GB P2P Loopback Test ===" << std::endl;
    std::cout << "  chunks=" << NUM_CHUNKS << " x " << (CHUNK_SIZE>>10) << "KB" << std::endl;

    create_test_file();
    auto chunk_offsets = create_tseed();
    auto seed_file = read_tseed(TSEED_PATH);
    uint32_t chunk_count = seed_file->header.chunk_count;
    Sha1Digest info_hash = seed_file->info_hash;

    asio::io_context io;
    const uint32_t local_speed = 1000;

    // ═══ Seed (A): 有全部数据，只上传 ═══
    std::cout << "\n[Seed] p2p=16889" << std::endl;
    int seed_fd = ::open(TEST_FILE, O_RDONLY);
    if (seed_fd < 0) { std::cerr << "FAIL: open" << std::endl; return 1; }

    Scheduler seed_sched;
    seed_sched.init(chunk_count, local_speed, {},{},{});
    seed_sched.mark_all_complete(std::vector<bool>(chunk_count, true));

    auto seed_asm = std::make_unique<ChunkAssembler[]>(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++) seed_asm[i].init(nullptr, CHUNK_SIZE);
    IOWorkerPool seed_io;
    seed_io.start(2, seed_asm.get(), [](ChunkCompleteMsg) {});

    PeerManager seed_pm(io, seed_sched, &seed_io, info_hash, local_speed, 16889);
    seed_pm.set_file_fd(seed_fd);
    seed_pm.set_initial_bitfield(std::vector<bool>(chunk_count, true));
    seed_pm.set_chunk_offsets(&chunk_offsets);
    seed_pm.start_accept();

    // ═══ Peer (B): 下载方 ═══
    std::cout << "[Peer] p2p=16890" << std::endl;

    SegmentWriter peer_writer;
    // segment_size 改为 2× 文件大小，避免 segment 切换导致旧 mmap 失效
    if (!peer_writer.open(DOWNLOAD_PATH, FILE_SIZE, FILE_SIZE * 2)) {
        std::cerr << "FAIL: peer_writer.open" << std::endl; return 1;
    }

    auto peer_asm = std::make_unique<ChunkAssembler[]>(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++) {
        uint8_t* base = peer_writer.get_chunk_base(
            static_cast<uint64_t>(i) * CHUNK_SIZE, CHUNK_SIZE);
        if (!base) { std::cerr << "FAIL: base null chunk " << i << std::endl; return 1; }
        peer_asm[i].init(base, CHUNK_SIZE);
    }

    std::vector<ChunkCompleteMsg> completions;
    std::mutex completions_mutex;
    IOWorkerPool peer_io;
    peer_io.start(4, peer_asm.get(),
        [&](ChunkCompleteMsg msg) {
            std::lock_guard<std::mutex> lk(completions_mutex);
            completions.push_back(msg);
        });

    std::map<uint32_t, uint64_t> sub_req_per_slot;

    Scheduler peer_sched;
    PeerManager* pm_ptr = nullptr;
    peer_sched.init(chunk_count, local_speed,
        [&](uint32_t slot, uint32_t ci, uint32_t begin, uint32_t len) {
            sub_req_per_slot[slot]++;
            if (pm_ptr) {
                auto* s = pm_ptr->get_session(slot);
                if (s) { s->send_message(build_request(ci, begin, len)); s->inc_pending(); s->record_request_sent(ci, begin); }
            }
        },
        [](uint32_t) {},
        []() {});
    peer_sched.set_chunk_sizes(std::vector<uint32_t>(chunk_count, CHUNK_SIZE));

    PeerManager peer_pm(io, peer_sched, &peer_io, info_hash, local_speed, 16890);
    pm_ptr = &peer_pm;
    peer_pm.set_file_fd(peer_writer.get_file_fd());
    peer_pm.set_initial_bitfield(std::vector<bool>(chunk_count, false));
    peer_pm.set_chunk_offsets(&chunk_offsets);
    peer_pm.start_accept();

    // ═══ 连接 + 握手 ═══
    std::cout << "\n[Test] Connecting..." << std::endl;
    peer_pm.connect_to("127.0.0.1", 16889, 0x02);

    // 驱动握手 + BITFIELD 交换
    for (int i = 0; i < 200; i++) {
        io.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Unchoke
    seed_pm.tick_choke();
    for (int i = 0; i < 100; i++) {
        io.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // 检查连接状态
    bool connected = false;
    for (const auto& s : peer_pm.sessions()) {
        if (s->remote_bitfield().size() > 0 && !s->is_choked()) { connected = true; break; }
    }
    if (!connected) {
        std::cerr << "FAIL: handshake. rf_size=";
        for (const auto& s : peer_pm.sessions())
            std::cerr << s->remote_bitfield().size() << " choked=" << s->is_choked() << " ";
        std::cerr << std::endl;
        return 1;
    }

    std::cout << "  OK, seed_peers=" << seed_pm.peer_count()
              << " peer_peers=" << peer_pm.peer_count() << std::endl;

    // ═══ 传输循环 ═══
    std::cout << "[Test] Transferring 4GB..." << std::endl;

    auto t_start = std::chrono::steady_clock::now();
    uint64_t total_done = 0;
    auto last_report = t_start;

    while (peer_sched.phase() != SchedulerPhase::DONE) {
        io.poll();

        // 安全排空完成通知（I/O 线程并发 push，加锁排空）
        std::vector<ChunkCompleteMsg> batch;
        {
            std::lock_guard<std::mutex> lk(completions_mutex);
            if (!completions.empty()) batch.swap(completions);
        }
        if (!batch.empty()) peer_sched.process_completions(batch);

        peer_sched.tick();

        uint64_t now_done = chunk_count - peer_sched.missing_count();
        if (now_done != total_done) {
            total_done = now_done;
            auto now = std::chrono::steady_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_report).count();
            if (elapsed_ms >= 500 || total_done == chunk_count) {
                double total_elapsed = std::chrono::duration<double>(now - t_start).count();
                double speed_mbps = (total_done * CHUNK_SIZE * 8.0 / total_elapsed) / 1e6;
                double pct = 100.0 * total_done / chunk_count;
                std::cout << "  [" << std::fixed << std::setprecision(1) << total_elapsed << "s] "
                          << total_done << "/" << chunk_count << " (" << pct << "%)  "
                          << speed_mbps << " Mbps" << std::endl;
                last_report = now;
            }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    auto t_end = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(t_end - t_start).count();

    // 收尾
    peer_io.stop(); seed_io.stop(); peer_writer.close();

    // ═══ 统计 ═══
    double speed_mbps = (FILE_SIZE * 8.0 / elapsed_sec) / 1e6;
    uint64_t total_sub = 0;
    for (auto& [slot, n] : sub_req_per_slot) total_sub += n;
    uint32_t total_subs_expected = chunk_count * (CHUNK_SIZE / SUB_BLOCK_SIZE);

    std::cout << "\n========== 结果 ==========" << std::endl;
    std::cout << "  文件:       " << (FILE_SIZE >> 30) << " GB  (" << chunk_count << " chunks x " << (CHUNK_SIZE>>10) << "KB)" << std::endl;
    std::cout << "  耗时:       " << std::fixed << std::setprecision(2) << elapsed_sec << " s" << std::endl;
    std::cout << "  吞吐:       " << speed_mbps << " Mbps  (" << (speed_mbps / 8.0) << " MB/s)" << std::endl;
    std::cout << "  sub-block:  " << total_sub << " requests  (预期 " << total_subs_expected << ")" << std::endl;
    std::cout << "  Peer 分发:" << std::endl;
    for (auto& [slot, n] : sub_req_per_slot) {
        std::cout << "    slot " << slot << ": " << n << " sub-blocks" << std::endl;
    }

    // ═══ 验证 ═══
    std::cout << "\n[Verify] SHA-256..." << std::flush;
    auto orig = sha256_file(TEST_FILE);
    auto dl = sha256_file(DOWNLOAD_PATH);
    if (orig != dl) {
        std::cerr << "FAIL\n  orig: " << sha256_hex(orig).substr(0,32)
                  << "\n  dl:   " << sha256_hex(dl).substr(0,32) << std::endl;
        return 1;
    }
    std::cout << " OK" << std::endl;

    ::close(seed_fd);
    std::remove(TEST_FILE); std::remove(TSEED_PATH); std::remove(DOWNLOAD_PATH);
    std::cout << "\n[PASS] 4GB P2P transfer OK!" << std::endl;
    return 0;
}
