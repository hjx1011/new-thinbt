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
#include <atomic>
#include <sstream>

using namespace thinbt;

// Reduced for local test runs: 30MB (originally 30GB in full CI)
static constexpr uint64_t FILE_SIZE    = 30ULL * 1024 * 1024;  // 30MB
static constexpr uint32_t CHUNK_SIZE   = 128 * 1024;
static constexpr uint32_t NUM_CHUNKS   = FILE_SIZE / CHUNK_SIZE;
static constexpr int    NUM_NODES      = 5;  // 1 seed + 4 leechers
static constexpr int    SEED_IDX       = 0;
static constexpr int    BASE_PORT      = 16889;

static const char* TEST_FILE     = "/tmp/thinbt_multi_src.bin";
static const char* TSEED_PATH    = "/tmp/thinbt_multi.tseed";

// 全局追踪: uploads[from_node][to_node] = sub-block 数量
static std::map<int, std::map<int, uint64_t>> g_uploads;
static std::mutex g_upload_mutex;

static void record_upload(int from_node, int to_node) {
    std::lock_guard<std::mutex> lk(g_upload_mutex);
    g_uploads[from_node][to_node]++;
}

static void create_test_file() {
    std::cout << "[Setup] generating " << (FILE_SIZE>>20) << "MB test file..." << std::flush;
    FILE* f = fopen(TEST_FILE, "wb");
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
void write_tseed(const std::string& output_path, const std::string& file_path,
                 const std::string& file_name, const std::string& announce_url,
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

struct Node {
    int id;
    int port;
    bool is_seed = false;
    std::unique_ptr<SegmentWriter> writer;
    int file_fd = -1;
    std::unique_ptr<ChunkAssembler[]> assemblers;
    std::unique_ptr<IOWorkerPool> io_pool;
    std::vector<ChunkCompleteMsg> completions;
    std::mutex completions_mutex;
    std::unique_ptr<Scheduler> scheduler;
    std::unique_ptr<PeerManager> peer_mgr;
    PeerManager* peer_mgr_ptr = nullptr;
};

int main() {
    std::cout << "=== " << NUM_NODES << "-Node P2P Mesh Test ===" << std::endl;
    std::cout << "  file=" << (FILE_SIZE>>20) << "MB  chunks=" << NUM_CHUNKS
              << "x" << (CHUNK_SIZE>>10) << "KB  nodes=" << NUM_NODES << std::endl;

    create_test_file();
    auto chunk_offsets = create_tseed();
    auto seed_file = read_tseed(TSEED_PATH);
    uint32_t chunk_count = seed_file->header.chunk_count;
    Sha1Digest info_hash = seed_file->info_hash;

    asio::io_context io;
    const uint32_t local_speed = 1000;

    // ── 创建所有节点 ──
    std::vector<Node> nodes(NUM_NODES);
    for (int i = 0; i < NUM_NODES; i++) {
        nodes[i].id = i;
        nodes[i].port = BASE_PORT + i;
        nodes[i].is_seed = (i == SEED_IDX);
    }

    // ── 初始化 Seed ──
    {
        auto& n = nodes[SEED_IDX];
        n.file_fd = ::open(TEST_FILE, O_RDONLY);
        n.assemblers.reset(new ChunkAssembler[chunk_count]);
        for (uint32_t j = 0; j < chunk_count; j++) n.assemblers[j].init(nullptr, CHUNK_SIZE);
        n.io_pool = std::make_unique<IOWorkerPool>();
        n.io_pool->start(2, n.assemblers.get(), [](ChunkCompleteMsg) {});
        n.scheduler = std::make_unique<Scheduler>();
        n.scheduler->init(chunk_count, local_speed, {},{},{});
        n.scheduler->mark_all_complete(std::vector<bool>(chunk_count, true));
        n.peer_mgr = std::make_unique<PeerManager>(
            io, *n.scheduler, n.io_pool.get(), info_hash, local_speed, n.port);
        n.peer_mgr->set_file_fd(n.file_fd);
        n.peer_mgr->set_initial_bitfield(std::vector<bool>(chunk_count, true));
        n.peer_mgr->set_chunk_offsets(&chunk_offsets);
        n.peer_mgr->start_accept();
        std::cout << "[Node 0] SEED  p2p=" << n.port << std::endl;
    }

    // ── 初始化 Leechers ──
    uint32_t leecher_count = NUM_NODES - 1;
    uint32_t chunks_per_leecher = chunk_count / leecher_count;

    for (int i = 1; i < NUM_NODES; i++) {
        auto& n = nodes[i];
        std::ostringstream path;
        path << "/tmp/thinbt_multi_dl_" << i << ".bin";

        n.writer = std::make_unique<SegmentWriter>();
        n.writer->open(path.str(), FILE_SIZE, 1024ULL * 1024 * 1024); // 1GB segment

        n.assemblers.reset(new ChunkAssembler[chunk_count]);
        for (uint32_t j = 0; j < chunk_count; j++) {
            uint8_t* base = n.writer->get_chunk_base(
                static_cast<uint64_t>(j) * CHUNK_SIZE, CHUNK_SIZE);
            n.assemblers[j].init(base, CHUNK_SIZE);
        }

        n.io_pool = std::make_unique<IOWorkerPool>();
        n.io_pool->start(4, n.assemblers.get(),
            [&n](ChunkCompleteMsg msg) {
                std::lock_guard<std::mutex> lk(n.completions_mutex);
                n.completions.push_back(msg);
            });

        n.scheduler = std::make_unique<Scheduler>();
        int my_id = i;
        uint32_t my_chunk_start = (i - 1) * chunks_per_leecher;
        uint32_t my_chunk_end   = (i == NUM_NODES - 1) ? chunk_count : my_chunk_start + chunks_per_leecher;

        n.scheduler->init(chunk_count, local_speed,
            [my_id, &n](uint32_t slot, uint32_t ci, uint32_t begin, uint32_t len) {
                if (n.peer_mgr_ptr) {
                    auto* sess = n.peer_mgr_ptr->get_session(slot);
                    if (sess) {
                        int from_port = sess->remote_port();
                        int from_node = from_port - BASE_PORT;
                        if (from_node >= 0 && from_node < NUM_NODES) {
                            record_upload(from_node, my_id);
                        }
                        sess->send_message(build_request(ci, begin, len));
                        sess->inc_pending();
                        sess->record_request_sent(ci, begin);
                    }
                }
            },
            [](uint32_t) {},
            []() {});
        n.scheduler->set_chunk_sizes(std::vector<uint32_t>(chunk_count, CHUNK_SIZE));

        // 预分配：每个 leecher 拥有 1/4 的 chunk（模拟已有的数据）
        std::vector<bool> init_bf(chunk_count, false);
        for (uint32_t c = my_chunk_start; c < my_chunk_end; c++)
            init_bf[c] = true;

        // 流式预读：1MB 批次（MAX_CHUNK_SIZE 限制），避免大缓冲区 OOM
        uint64_t preload_offset = static_cast<uint64_t>(my_chunk_start) * CHUNK_SIZE;
        uint64_t preload_len    = static_cast<uint64_t>(my_chunk_end - my_chunk_start) * CHUNK_SIZE;
        constexpr size_t BUF_SZ = 1ULL * 1024 * 1024;  // MAX_CHUNK_SIZE
        std::vector<uint8_t> buf(BUF_SZ);
        for (uint64_t off = 0; off < preload_len; off += BUF_SZ) {
            size_t chunk = std::min(BUF_SZ, static_cast<size_t>(preload_len - off));
            ssize_t nr = thinbt_pread(nodes[SEED_IDX].file_fd, buf.data(),
                                      chunk, static_cast<off_t>(preload_offset + off));
            if (nr > 0) {
                uint8_t* dst = n.writer->get_chunk_base(preload_offset + off, nr);
                if (dst) memcpy(dst, buf.data(), nr);
            }
        }

        n.peer_mgr = std::make_unique<PeerManager>(
            io, *n.scheduler, n.io_pool.get(), info_hash, local_speed, n.port);
        n.peer_mgr_ptr = n.peer_mgr.get();
        n.peer_mgr->set_file_fd(n.writer->get_file_fd());
        n.peer_mgr->set_initial_bitfield(init_bf);
        n.peer_mgr->set_chunk_offsets(&chunk_offsets);
        n.peer_mgr->start_accept();
        n.scheduler->mark_all_complete(init_bf);

        std::cout << "[Node " << i << "] LEECH p2p=" << n.port
                  << "  preloaded=[" << my_chunk_start << "," << my_chunk_end
                  << ") (" << (my_chunk_end - my_chunk_start) << " chunks)" << std::endl;
    }

    // ── 建立全网状连接 ──
    std::cout << "\n[Mesh] connecting..." << std::endl;
    for (int i = 1; i < NUM_NODES; i++) {
        for (int j = 0; j < NUM_NODES; j++) {
            if (i == j) continue;
            nodes[i].peer_mgr->connect_to("127.0.0.1", BASE_PORT + j, 0x02);
        }
    }

    // ── 握手 + Unchoke ──
    for (int iter = 0; iter < 500; iter++) {
        io.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (iter == 100) {
            for (auto& n : nodes) n.peer_mgr->tick_choke();
        }
    }

    // 连接状态
    std::cout << "\n[Connect] " << std::flush;
    for (int i = 0; i < NUM_NODES; i++) {
        int c = 0;
        for (auto& s : nodes[i].peer_mgr->sessions())
            if (s->remote_bitfield().size() > 0) c++;
        std::cout << "n" << i << ":" << c << " ";
    }
    std::cout << std::endl;

    // ── 传输 ──
    std::cout << "\n[Transfer]" << std::endl;
    auto t_start = std::chrono::steady_clock::now();
    auto last_report = t_start;
    // 不周期性调用 tick_choke — 首次 unchoke 后保持开放，避免死锁
    // 真实网络中由 10s choke interval + optimistic unchoke 自动恢复

    while (true) {
        io.poll();

        bool all_done = true;
        for (int i = 1; i < NUM_NODES; i++) {
            auto& n = nodes[i];
            std::vector<ChunkCompleteMsg> batch;
            {
                std::lock_guard<std::mutex> lk(n.completions_mutex);
                if (!n.completions.empty()) batch.swap(n.completions);
            }
            if (!batch.empty()) n.scheduler->process_completions(batch);
            n.scheduler->tick();
            if (n.scheduler->phase() != SchedulerPhase::DONE) all_done = false;
        }

        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_report).count();
        if (elapsed_ms >= 1000) {
            double total_elapsed = std::chrono::duration<double>(now - t_start).count();
            std::cout << "  [" << std::fixed << std::setprecision(1) << total_elapsed << "s] ";
            for (int i = 1; i < NUM_NODES; i++) {
                int pct = (chunk_count - nodes[i].scheduler->missing_count()) * 100 / chunk_count;
                std::cout << "n" << i << ":" << pct << "% ";
            }
            std::cout << std::endl;
            last_report = now;
        }

        if (all_done) break;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    auto t_end = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(t_end - t_start).count();

    // ── 收尾 ──
    for (auto& n : nodes) { n.io_pool->stop(); if (n.writer) n.writer->close(); }

    // ═══ 统计 ═══
    std::cout << "\n========== P2P 传输矩阵 (sub-blocks) ==========" << std::endl;
    std::cout << "        ";
    for (int j = 1; j < NUM_NODES; j++) std::cout << "  Node" << j << "   ";
    std::cout << "   总计上传" << std::endl;

    uint64_t grand_total = 0;
    uint64_t total_from_seed = 0;
    uint64_t total_p2p = 0;

    for (int from = 0; from < NUM_NODES; from++) {
        if (from == 0) std::cout << "Seed   ";
        else std::cout << "Node" << from << "  ";

        uint64_t row_total = 0;
        for (int to = 1; to < NUM_NODES; to++) {
            uint64_t n = g_uploads[from][to];
            row_total += n;
            if (n > 0) std::cout << std::setw(8) << n << " ";
            else std::cout << "       - ";
        }
        std::cout << std::setw(10) << row_total;
        grand_total += row_total;
        if (from == 0) total_from_seed += row_total;
        else total_p2p += row_total;
        std::cout << std::endl;
    }

    uint32_t expected_subs = chunk_count * (CHUNK_SIZE / SUB_BLOCK_SIZE);
    double total_data_mb = grand_total * SUB_BLOCK_SIZE / (1024.0 * 1024.0);
    double speed_mbps = (total_data_mb * 8.0) / elapsed_sec;
    double p2p_pct = grand_total > 0 ? (total_p2p * 100.0 / grand_total) : 0;

    std::cout << "\n  每个 leecher 应接收: " << expected_subs << " sub-blocks" << std::endl;
    for (int to = 1; to < NUM_NODES; to++) {
        uint64_t recv = 0;
        for (int from = 0; from < NUM_NODES; from++) recv += g_uploads[from][to];
        std::cout << "    Node" << to << ": " << recv << " sub-blocks";
        if (recv != expected_subs) std::cout << " ⚠";
        std::cout << std::endl;
    }

    std::cout << "\n  总传输量:  " << grand_total << " sub-blocks (" << total_data_mb << " MB)" << std::endl;
    std::cout << "  来自 Seed:  " << total_from_seed << " (" << (100.0 - p2p_pct) << "%)" << std::endl;
    std::cout << "  P2P 互传:   " << total_p2p << " (" << p2p_pct << "%)" << std::endl;
    std::cout << "  总耗时:     " << elapsed_sec << " s" << std::endl;
    std::cout << "  聚合吞吐:   " << speed_mbps << " Mbps (" << (speed_mbps/8.0) << " MB/s)" << std::endl;

    // ── 验证 ──
    std::cout << "\n[Verify] SHA-256..." << std::flush;
    auto orig = sha256_file(TEST_FILE);
    bool all_ok = true;
    for (int i = 1; i < NUM_NODES; i++) {
        std::ostringstream path;
        path << "/tmp/thinbt_multi_dl_" << i << ".bin";
        if (sha256_file(path.str()) != orig) {
            std::cerr << "\n  Node" << i << " FAIL" << std::endl;
            all_ok = false;
        }
    }
    if (all_ok) std::cout << " ALL " << (NUM_NODES-1) << " nodes OK" << std::endl;

    // ── 清理 ──
    ::close(nodes[SEED_IDX].file_fd);
    std::remove(TEST_FILE); std::remove(TSEED_PATH);
    for (int i = 1; i < NUM_NODES; i++) {
        std::ostringstream p; p << "/tmp/thinbt_multi_dl_" << i << ".bin";
        std::remove(p.str().c_str());
    }
    std::cout << (all_ok ? "\n[PASS]" : "\n[FAIL]") << " " << NUM_NODES
              << "-node mesh P2P test!" << std::endl;
    return all_ok ? 0 : 1;
}
