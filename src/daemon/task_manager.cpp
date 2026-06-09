#include "task_manager.hpp"
#include "peer_manager.hpp"
#include "tracker_client.hpp"
#include "common/net_util.hpp"
#include "common/hash.hpp"
#include "cdc/fastcdc.hpp"
#include "seed/seed_reader.hpp"
#include <sstream>
#include <iomanip>
#include <random>
#include <ctime>
#include <thread>
#include <iostream>
#include <fcntl.h>

namespace thinbt {

std::string TaskManager::gen_task_id() {
    static std::mt19937 rng(std::random_device{}());
    static std::uniform_int_distribution<int> dist(0, 15);
    std::ostringstream oss;
    for (int i = 0; i < 8; i++) oss << std::hex << dist(rng);
    return oss.str();
}

TaskManager::TaskManager(asio::io_context& io, uint16_t p2p_port,
                          const std::string& tracker_host, uint16_t tracker_port)
    : io_(io), p2p_port_(p2p_port), tracker_host_(tracker_host), tracker_port_(tracker_port), tracker_(tracker_port) {}

std::string TaskManager::cmd_seed(const std::string& seed_path, const std::string& file_path) {
    auto seed = read_tseed(seed_path);
    auto tid = gen_task_id();
    auto task = std::make_unique<ActiveTask>();
    task->task_id = tid;
    task->is_seed = true;
    task->file_path = file_path;
    task->seed_path = seed_path;
    task->seed = std::move(seed);

    std::string info_hash_hex = sha1_hex(task->seed->info_hash);
    uint32_t chunk_count = task->seed->header.chunk_count;

    // 构建 chunk 偏移表（给 sendfile 算文件位置用）
    task->chunk_offsets.reserve(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->chunk_offsets.push_back(task->seed->chunks[i].offset);

    // 打开源文件（只读，供 sendfile 使用）
#ifdef _WIN32
    task->seed_fd = ::_open(file_path.c_str(), _O_RDONLY | _O_BINARY);
#else
    task->seed_fd = ::open(file_path.c_str(), O_RDONLY);
#endif
    if (task->seed_fd < 0)
        return R"({"status":"error","error":"cannot open file for seeding"})";

    // Allocate assemblers (seed 只发不收，用占位内存)
    auto* raw_asm = new ChunkAssembler[chunk_count];
    task->assemblers.reset(raw_asm);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->assemblers[i].init(nullptr, task->seed->chunks[i].length);

    // I/O pool
    uint32_t io_threads = std::min(std::max(2u, std::thread::hardware_concurrency() / 2), 8u);
    task->io_pool = std::make_unique<IOWorkerPool>();
    task->io_pool->start(io_threads, task->assemblers.get(),
        [&c = task->completions](ChunkCompleteMsg msg) { c.push_back(msg); });

    // Scheduler
    task->scheduler = std::make_unique<Scheduler>();
    task->scheduler->init(chunk_count, 1000,
        [](uint32_t, uint32_t, uint32_t, uint32_t){},
        [](uint32_t){});

    // 设置 chunk 大小表（CDC 变长 chunk 的 sub-block 计数）
    {
        std::vector<uint32_t> sizes(chunk_count);
        for (uint32_t i = 0; i < chunk_count; i++)
            sizes[i] = task->seed->chunks[i].length;
        task->scheduler->set_chunk_sizes(sizes);
    }

    // 创建 PeerManager
    task->peer_mgr = std::make_unique<PeerManager>(
        io_, *task->scheduler, task->io_pool.get(),
        task->seed->info_hash, 1000, p2p_port_);

    // Seed 拥有完整文件，设置全 1 bitfield + 文件 fd + chunk 偏移
    std::vector<bool> full_bf(chunk_count, true);
    task->peer_mgr->set_initial_bitfield(full_bf);
    task->peer_mgr->set_file_fd(task->seed_fd);
    task->peer_mgr->set_chunk_offsets(&task->chunk_offsets);
    task->peer_mgr->start_accept();

    // 用能访问 PeerManager 的真实回调重新初始化 scheduler
    auto* pm = task->peer_mgr.get();
    task->scheduler->init(chunk_count, 1000,
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_request(chunk_idx, begin, length));
                sess->inc_pending();
            }
        },
        [pm](uint32_t chunk_idx) {
            auto have_msg = build_have(chunk_idx);
            for (auto& s : pm->sessions()) s->send_message(have_msg);
        });

    tasks_[tid] = std::move(task);

    return R"({"status":"ok","data":{"task_id":")" + tid + R"("}})";
}

std::string TaskManager::cmd_add(const std::string& seed_path, const std::string& save_path) {
    auto seed = read_tseed(seed_path);
    auto tid = gen_task_id();
    auto task = std::make_unique<ActiveTask>();
    task->task_id = tid;
    task->seed = std::move(seed);
    task->file_path = save_path;
    task->seed_path = seed_path;

    uint32_t chunk_count = task->seed->header.chunk_count;
    uint64_t file_size = task->seed->header.file_size;

    // 构建 chunk 偏移表
    task->chunk_offsets.reserve(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->chunk_offsets.push_back(task->seed->chunks[i].offset);

    // 创建 SegmentWriter（预分配 + mmap 顺序写，HDD 友好）
    task->writer = std::make_unique<SegmentWriter>();
    if (!task->writer->open(save_path, file_size))
        return R"({"status":"error","error":"cannot create output file"})";

    // 用 mmap 映射的真实地址初始化每个 chunk 的 assembler
    auto* raw_asm = new ChunkAssembler[chunk_count];
    task->assemblers.reset(raw_asm);
    for (uint32_t i = 0; i < chunk_count; i++) {
        uint64_t chunk_off = task->seed->chunks[i].offset;
        uint32_t chunk_len = task->seed->chunks[i].length;
        uint8_t* base = task->writer->get_chunk_base(chunk_off, chunk_len);
        if (!base) {
            return R"({"status":"error","error":"mmap chunk base failed"})";
        }
        task->assemblers[i].init(base, chunk_len);
    }

    uint32_t io_threads = std::min(std::max(2u, std::thread::hardware_concurrency() / 2), 8u);
    task->io_pool = std::make_unique<IOWorkerPool>();
    task->io_pool->start(io_threads, task->assemblers.get(),
        [&c = task->completions](ChunkCompleteMsg msg) { c.push_back(msg); });

    task->scheduler = std::make_unique<Scheduler>();
    task->scheduler->init(chunk_count, 1000,
        [](uint32_t, uint32_t, uint32_t, uint32_t){},
        [](uint32_t){});

    // 设置 chunk 大小表
    {
        std::vector<uint32_t> sizes(chunk_count);
        for (uint32_t i = 0; i < chunk_count; i++)
            sizes[i] = task->seed->chunks[i].length;
        task->scheduler->set_chunk_sizes(sizes);
    }

    // 创建 PeerManager（leecher 初始 bitfield 为空）
    task->peer_mgr = std::make_unique<PeerManager>(
        io_, *task->scheduler, task->io_pool.get(),
        task->seed->info_hash, 1000, p2p_port_);

    std::vector<bool> empty_bf(chunk_count, false);
    task->peer_mgr->set_initial_bitfield(empty_bf);
    task->peer_mgr->set_file_fd(task->writer->get_file_fd());
    task->peer_mgr->set_chunk_offsets(&task->chunk_offsets);
    task->peer_mgr->start_accept();

    // 接线 scheduler 回调
    auto* pm = task->peer_mgr.get();
    task->scheduler->init(chunk_count, 1000,
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_request(chunk_idx, begin, length));
                sess->inc_pending();
            }
        },
        [pm](uint32_t chunk_idx) {
            auto have_msg = build_have(chunk_idx);
            for (auto& s : pm->sessions()) s->send_message(have_msg);
        });

    tasks_[tid] = std::move(task);

    return R"({"status":"ok","data":{"task_id":")" + tid + R"("}})";
}

std::string TaskManager::cmd_update(const std::string& new_seed, const std::string& new_file,
                                     const std::string& old_seed, const std::string& old_file) {
    // 增量更新：窗口 4 实现完整逻辑
    (void)new_seed; (void)new_file; (void)old_seed; (void)old_file;
    return R"({"status":"ok","data":{"task_id":"00000001","msg":"update stub"}})";
}

std::vector<TaskInfo> TaskManager::cmd_list() {
    std::vector<TaskInfo> result;
    for (auto& [tid, t] : tasks_) {
        TaskInfo info;
        info.task_id   = tid;
        info.state     = t->is_seed ? "seeding" : "downloading";
        info.file_path = t->file_path;
        info.seed_path = t->seed_path;
        info.bytes_done = t->bytes_done;
        if (t->seed)
            info.progress = t->seed->header.file_size > 0
                ? static_cast<double>(t->bytes_done) / t->seed->header.file_size : 0.0;
        result.push_back(info);
    }
    return result;
}

std::string TaskManager::cmd_remove(const std::string& task_id, bool /*force*/) {
    if (tasks_.erase(task_id))
        return R"({"status":"ok"})";
    return R"({"status":"error","error":"task not found"})";
}

void TaskManager::tick() {
    for (auto& [tid, t] : tasks_) {
        if (t->io_pool && t->scheduler) {
            t->scheduler->tick();
            t->scheduler->process_completions(t->completions);
        }
    }
}

void TaskManager::tick_tracker_announce(asio::io_context& io) {
    for (auto& [tid, t] : tasks_) {
        if (!t->seed) continue;
        std::string info_hash_hex = sha1_hex(t->seed->info_hash);

        // 优先级: CLI 参数 > seed announce_url > 默认回退
        std::string host = tracker_host_;
        uint16_t port = tracker_port_;

        if (host.empty() && t->seed) {
            auto proto_pos = t->seed->announce_url.find("thinbt://");
            if (proto_pos != std::string::npos) {
                auto host_start = proto_pos + 9;
                auto colon = t->seed->announce_url.find(':', host_start);
                auto slash = t->seed->announce_url.find('/', host_start);
                if (colon != std::string::npos && slash != std::string::npos) {
                    host = t->seed->announce_url.substr(host_start, colon - host_start);
                    port = static_cast<uint16_t>(std::stoi(
                        t->seed->announce_url.substr(colon + 1, slash - colon - 1)));
                }
            }
        }
        if (host.empty()) host = "127.0.0.1";

        // Create or reuse TrackerClient
        if (!t->tracker_client) {
            t->tracker_client = std::make_shared<TrackerClient>(
                io, info_hash_hex, p2p_port_, 1000);
        }

        // PeerManager 应在 cmd_seed/cmd_add 中已创建
        if (!t->peer_mgr) continue;

        t->tracker_client->announce(host, port,
            [&t = *t](const std::vector<PexPeer>& peers) {
                for (auto& p : peers) {
                    struct in_addr ia; ia.s_addr = p.ip;
                    std::string ip = inet_ntoa(ia);
                    t.peer_mgr->connect_to(ip, p.port, p.flags);
                }
            });
    }
}

void TaskManager::tick_choke_all() {
    for (auto& [tid, t] : tasks_) {
        if (t->peer_mgr) t->peer_mgr->tick_choke();
    }
}

void TaskManager::tick_pex_all() {
    for (auto& [tid, t] : tasks_) {
        if (t->peer_mgr) t->peer_mgr->tick_pex();
    }
}

} // namespace thinbt
