#include "task_manager.hpp"
#include "common/net_util.hpp"
#include "cdc/fastcdc.hpp"
#include "seed/seed_reader.hpp"
#include <sstream>
#include <iomanip>
#include <random>
#include <ctime>

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
    auto tid = gen_task_id();
    auto task = std::make_unique<ActiveTask>();
    task->task_id = tid;
    task->is_seed = true;
    task->file_path = file_path;
    task->seed_path = seed_path;
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

    uint32_t chunk_count = task->seed->header.chunk_count;

    // Allocate assemblers (backed by real file in integration)
    auto* raw_asm = new ChunkAssembler[chunk_count];
    task->assemblers.reset(raw_asm);

    // Dummy buffer for stubs
    static std::vector<uint8_t> dummy_buf(128 * 1024, 0);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->assemblers[i].init(dummy_buf.data(), task->seed->chunks[i].length);

    // Start I/O pool
    uint32_t io_threads = std::min(std::max(2u, std::thread::hardware_concurrency() / 2), 8u);
    task->io_pool = std::make_unique<IOWorkerPool>();
    task->io_pool->start(io_threads, task->assemblers.get(),
        [&c = task->completions](ChunkCompleteMsg msg) { c.push_back(msg); });

    // Scheduler
    task->scheduler = std::make_unique<Scheduler>();
    task->scheduler->init(chunk_count, 1000,
        [](uint32_t, uint32_t, uint32_t, uint32_t){},
        [](uint32_t){});

    // Peer Manager
    // PeerManager is global (one per daemon), not per-task

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
            t->io_pool->stop();
            t->scheduler->process_completions(t->completions);
        }
    }
}

} // namespace thinbt
