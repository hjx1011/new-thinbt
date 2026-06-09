#ifndef THINBT_TASK_MANAGER_HPP
#define THINBT_TASK_MANAGER_HPP

#include "common/platform.hpp"
#include "seed/tseed.hpp"
#include "chunk_assembler.hpp"
#include "io_worker.hpp"
#include "verify_worker.hpp"
#include "scheduler.hpp"
#include "segment_io.hpp"
#include "tracker_server.hpp"
#include <memory>
#include <string>
#include <map>
#include <vector>

namespace asio { class io_context; }

namespace thinbt {

class PeerManager;
class TrackerClient;

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
    TaskManager(asio::io_context& io, uint16_t p2p_port,
                const std::string& tracker_host = "",
                uint16_t tracker_port = 8080);

    std::string cmd_seed(const std::string& seed_path, const std::string& file_path);
    std::string cmd_add(const std::string& seed_path, const std::string& save_path);
    std::vector<TaskInfo> cmd_list();
    std::string cmd_remove(const std::string& task_id, bool force);
    std::string cmd_update(const std::string& new_seed, const std::string& new_file,
                           const std::string& old_seed, const std::string& old_file);
    std::string cmd_peers(const std::string& task_id);

    TrackerServer& tracker() { return tracker_; }
    void tick();

    // Periodic ticks called from heartbeat
    void tick_tracker_announce(asio::io_context& io);
    void tick_choke_all();
    void tick_pex_all();

    static std::string gen_task_id();

private:
    struct ActiveTask {
        std::string task_id;
        std::unique_ptr<TSeedFile> seed;
        std::string file_path;
        std::string seed_path;
        bool is_seed = false;
        std::string state;                       // seeding / downloading / waiting / complete / error
        std::string started_at;                  // ISO 8601 启动时间
        std::string finished_at;                 // ISO 8601 完成时间

        std::unique_ptr<ChunkAssembler[]> assemblers;
        std::unique_ptr<IOWorkerPool> io_pool;
        std::unique_ptr<VerifyWorkerPool> verify_pool;
        std::unique_ptr<Scheduler> scheduler;
        std::unique_ptr<PeerManager> peer_mgr;
        std::shared_ptr<TrackerClient> tracker_client;

        // 文件 I/O
        int seed_fd = -1;                        // seed: 只读 fd，供 sendfile 使用
        std::unique_ptr<SegmentWriter> writer;    // download: mmap 分段写入
        std::vector<uint64_t> chunk_offsets;      // chunk 在文件中的偏移（给 sendfile 算位置）

        uint64_t bytes_done = 0;
        double speed_ema = 0.0;
        uint64_t last_bytes_done = 0;            // 上一 tick 的 bytes_done，用于计算速度
        std::atomic<bool> tracker_dead{false};   // Tracker 重试耗尽标记
    };

    asio::io_context& io_;
    uint16_t p2p_port_;
    std::string tracker_host_;
    uint16_t tracker_port_;
    TrackerServer tracker_;
    std::map<std::string, std::unique_ptr<ActiveTask>> tasks_;
};

} // namespace thinbt
#endif
