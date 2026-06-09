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

namespace thinbt {

std::string TaskManager::gen_task_id() {
    static std::mt19937 rng(std::random_device{}());
    static std::uniform_int_distribution<int> dist(0, 15);
    std::ostringstream oss;
    for (int i = 0; i < 8; i++) oss << std::hex << dist(rng);
    return oss.str();
}

TaskManager::TaskManager(uint16_t p2p_port)
    : p2p_port_(p2p_port), tracker_(8080) {}

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

    // Allocate assemblers
    auto* raw_asm = new ChunkAssembler[chunk_count];
    task->assemblers.reset(raw_asm);
    static std::vector<uint8_t> dummy_buf(128 * 1024, 0);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->assemblers[i].init(dummy_buf.data(), task->seed->chunks[i].length);

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

    tasks_[tid] = std::move(task);

    // Tracker announce will be triggered on first heartbeat tick

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

    auto* raw_asm = new ChunkAssembler[chunk_count];
    task->assemblers.reset(raw_asm);
    static std::vector<uint8_t> dummy_buf(128 * 1024, 0);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->assemblers[i].init(dummy_buf.data(), task->seed->chunks[i].length);

    uint32_t io_threads = std::min(std::max(2u, std::thread::hardware_concurrency() / 2), 8u);
    task->io_pool = std::make_unique<IOWorkerPool>();
    task->io_pool->start(io_threads, task->assemblers.get(),
        [&c = task->completions](ChunkCompleteMsg msg) { c.push_back(msg); });

    task->scheduler = std::make_unique<Scheduler>();
    task->scheduler->init(chunk_count, 1000,
        [](uint32_t, uint32_t, uint32_t, uint32_t){},
        [](uint32_t){});

    tasks_[tid] = std::move(task);

    return R"({"status":"ok","data":{"task_id":")" + tid + R"("}})";
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
            t->scheduler->process_completions(t->completions);
        }
    }
}

void TaskManager::tick_tracker_announce(asio::io_context& io) {
    for (auto& [tid, t] : tasks_) {
        if (!t->seed) continue;
        std::string info_hash_hex = sha1_hex(t->seed->info_hash);

        // Parse tracker URL from seed
        std::string host = "192.168.177.56";
        uint16_t port = 8080;
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

        // Create or reuse TrackerClient
        if (!t->tracker_client) {
            t->tracker_client = std::make_shared<TrackerClient>(
                io, info_hash_hex, p2p_port_, 1000);
        }

        // Create PeerManager if not yet
        if (!t->peer_mgr) {
            t->peer_mgr = std::make_unique<PeerManager>(
                io, *t->scheduler, t->io_pool.get(),
                t->seed->info_hash, 1000, p2p_port_);
            t->peer_mgr->start_accept();
        }

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
