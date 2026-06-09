#ifndef THINBT_TASK_MANAGER_HPP
#define THINBT_TASK_MANAGER_HPP

#include "common/platform.hpp"
#include "seed/tseed.hpp"
#include "chunk_assembler.hpp"
#include "io_worker.hpp"
#include "scheduler.hpp"
#include "tracker_server.hpp"
#include <memory>
#include <string>
#include <map>
#include <vector>

namespace thinbt {

struct TaskInfo {
    std::string task_id;
    std::string state;
    double progress = 0.0;
    uint64_t bytes_done = 0;
    double speed_mib_s = 0.0;
    std::string file_path;
    std::string seed_path;
    std::string started_at;
    std::string finished_at;
};

class TaskManager {
public:
    TaskManager(uint16_t p2p_port);

    std::string cmd_seed(const std::string& seed_path, const std::string& file_path);
    std::string cmd_add(const std::string& seed_path, const std::string& save_path);
    std::vector<TaskInfo> cmd_list();
    std::string cmd_remove(const std::string& task_id, bool force);

    TrackerServer& tracker() { return tracker_; }
    void tick();

    static std::string gen_task_id();

private:
    struct ActiveTask {
        std::string task_id;
        std::unique_ptr<TSeedFile> seed;
        std::string file_path;
        std::string seed_path;
        bool is_seed = false;

        std::unique_ptr<ChunkAssembler[]> assemblers;
        std::unique_ptr<IOWorkerPool> io_pool;
        std::vector<ChunkCompleteMsg> completions;
        std::unique_ptr<Scheduler> scheduler;

        uint64_t bytes_done = 0;
        double speed_ema = 0.0;
    };

    uint16_t p2p_port_;
    TrackerServer tracker_;
    std::map<std::string, std::unique_ptr<ActiveTask>> tasks_;
};

} // namespace thinbt
#endif
