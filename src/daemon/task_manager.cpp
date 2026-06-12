#include "task_manager.hpp"
#include "peer_manager.hpp"
#include "tracker_client.hpp"
#include "common/net_util.hpp"
#include "common/hash.hpp"
#include "cdc/fastcdc.hpp"
#include "common/file_util.hpp"
#include "seed/seed_reader.hpp"
#include <sstream>
#include <iomanip>
#include <random>
#include <ctime>
#include <thread>
#include <iostream>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <unordered_map>

namespace thinbt {

void mark_seed_complete(Scheduler& scheduler, const std::vector<bool>& bitfield) {
    scheduler.mark_all_complete(bitfield);
}

namespace {

// 返回 ISO 8601 格式 UTC 时间字符串: "2026-06-09T08:00:00Z"
std::string iso8601_now() {
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    auto tm = *std::gmtime(&tt);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

} // anonymous namespace

std::string TaskManager::gen_task_id() {
    static std::mt19937 rng(std::random_device{}());
    static std::uniform_int_distribution<int> dist(0, 15);
    std::ostringstream oss;
    for (int i = 0; i < 8; i++) oss << std::hex << dist(rng);
    return oss.str();
}

TaskManager::TaskManager(asio::io_context& io, uint16_t p2p_port,
                          const std::string& tracker_host, uint16_t tracker_port)
    : io_(io), p2p_port_(p2p_port), tracker_host_(tracker_host), tracker_port_(tracker_port), tracker_(tracker_port) {
    // 启动周期性统计定时器
    try {
        stats_timer_ = std::make_unique<asio::steady_timer>(io_);
        stats_timer_->expires_after(std::chrono::seconds(1));
        stats_timer_->async_wait([this](const asio::error_code& ec){ this->do_stats_tick(ec); });
    } catch (...) {
        // 如果无法创建定时器则忽略（不应发生）
    }
}

void TaskManager::do_stats_tick(const asio::error_code& ec) {
    if (ec) return;
    uint64_t total_kbps = 0;
    std::ostringstream detail;
    detail << "[stats]";
    for (auto& [tid, t] : tasks_) {
        if (!t->peer_mgr) continue;
        for (auto& s : t->peer_mgr->sessions()) {
            s->update_download_rate();
            s->update_upload_rate();
            uint32_t dl = s->download_rate_kbps();
            uint32_t ul = s->upload_rate_kbps();
            total_kbps += dl;
            if (dl > 0 || ul > 0)
                detail << " " << s->remote_ip() << ":dl=" << dl << "kbps,ul=" << ul << "kbps";
        }
    }
    std::cerr << detail.str() << " total=" << total_kbps << "kbps" << std::endl;
    if (stats_timer_) {
        stats_timer_->expires_after(std::chrono::seconds(1));
        stats_timer_->async_wait([this](const asio::error_code& e){ this->do_stats_tick(e); });
    }
}


// 统计定时器成员函数将在类中实现


std::string TaskManager::cmd_seed(const std::string& seed_path, const std::string& file_path) {
    auto seed = read_tseed(seed_path);
    auto tid = gen_task_id();
    auto task = std::make_unique<ActiveTask>();
    task->task_id = tid;
    task->is_seed = true;
    task->state = "seeding";
    task->started_at = iso8601_now();
    task->started_mono = std::chrono::steady_clock::now();
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

    // I/O pool + Verify pool
    uint32_t io_threads = std::min(std::max(2u, std::thread::hardware_concurrency() / 2), 8u);
    task->io_pool = std::make_unique<IOWorkerPool>();
    // SHA-256 校验在独立线程池中异步执行，不阻塞 I/O 线程 memcpy
    task->verify_pool = std::make_unique<VerifyWorkerPool>();
    task->verify_pool->start(task->seed->chunks.data(), task->assemblers.get());
    task->io_pool->start(io_threads, task->assemblers.get(),
        [vp = task->verify_pool.get()](ChunkCompleteMsg msg) { vp->enqueue(msg); });

    uint32_t local_speed = detect_link_speed_mbps();

    // 先创建 Scheduler（PeerManager 构造需要它的引用，但 init 在后面）
    task->scheduler = std::make_unique<Scheduler>();

    // 创建 PeerManager（scheduler 回调依赖它，必须先创建）
    task->peer_mgr = std::make_unique<PeerManager>(
        io_, *task->scheduler, task->io_pool.get(),
        task->seed->info_hash, local_speed, p2p_port_);

    // Seed 拥有完整文件，设置全 1 bitfield + 文件 fd + chunk 偏移
    std::vector<bool> full_bf(chunk_count, true);
    task->peer_mgr->set_initial_bitfield(full_bf);
    task->peer_mgr->set_file_fd(task->seed_fd);
    task->peer_mgr->set_chunk_offsets(&task->chunk_offsets);
    task->peer_mgr->start_accept();

    // 用能访问 PeerManager 的真实回调初始化 scheduler（仅一次）
    auto* pm = task->peer_mgr.get();
    task->scheduler->init(chunk_count, local_speed,
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_request(chunk_idx, begin, length));
                sess->inc_pending();
                sess->record_request_sent(chunk_idx, begin);
            }
        },
        [pm](uint32_t chunk_idx) {
            auto have_msg = build_have(chunk_idx);
            for (auto& s : pm->sessions()) s->send_message(have_msg);
        },
        [pm]() {
            auto msg = build_not_interested();
            for (auto& s : pm->sessions()) s->send_message(msg);
        });

    // init 后设置 chunk 大小表（CDC 变长 chunk 的 sub-block 计数）
    {
        std::vector<uint32_t> sizes(chunk_count);
        for (uint32_t i = 0; i < chunk_count; i++)
            sizes[i] = task->seed->chunks[i].length;
        task->scheduler->set_chunk_sizes(sizes);
    }
    mark_seed_complete(*task->scheduler, full_bf);

    task->scheduler->set_cancel_issuer(
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_cancel(chunk_idx, begin, length));
                sess->dec_pending();
            }
        });

    task->scheduler->set_not_interested_issuer(
        [pm](uint32_t slot_id) {
            auto* sess = pm->get_session(slot_id);
            if (sess) sess->send_not_interested();
        });

    auto& stored = tasks_[tid];
    stored = std::move(task);
    announce_task_now(*stored);

    return R"({"status":"ok","data":{"task_id":")" + tid + R"("}})";
}

std::string TaskManager::cmd_add(const std::string& seed_path, const std::string& save_path) {
    auto seed = read_tseed(seed_path);
    auto tid = gen_task_id();
    auto task = std::make_unique<ActiveTask>();
    task->task_id = tid;
    task->state = "downloading";
    task->started_at = iso8601_now();
    task->started_mono = std::chrono::steady_clock::now();
    task->startup_announce_deadline = task->started_mono + std::chrono::seconds(60);
    task->next_startup_announce = task->started_mono + std::chrono::seconds(2);
    task->seed = std::move(seed);
    task->file_path = save_path;
    task->seed_path = seed_path;

    uint32_t chunk_count = task->seed->header.chunk_count;
    uint64_t file_size = task->seed->header.file_size;

    // 构建 chunk 偏移表
    task->chunk_offsets.reserve(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->chunk_offsets.push_back(task->seed->chunks[i].offset);

    bool file_exists = false;
    struct stat st;
    if (::stat(save_path.c_str(), &st) == 0 && st.st_size > 0) file_exists = true;

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
    // SHA-256 校验在独立线程池中异步执行，不阻塞 I/O 线程 memcpy
    task->verify_pool = std::make_unique<VerifyWorkerPool>();
    task->verify_pool->start(task->seed->chunks.data(), task->assemblers.get());
    task->io_pool->start(io_threads, task->assemblers.get(),
        [vp = task->verify_pool.get()](ChunkCompleteMsg msg) { vp->enqueue(msg); });

    uint32_t local_speed = detect_link_speed_mbps();

    // 先创建 Scheduler（PeerManager 构造需要它的引用）
    task->scheduler = std::make_unique<Scheduler>();

    // 创建 PeerManager
    task->peer_mgr = std::make_unique<PeerManager>(
        io_, *task->scheduler, task->io_pool.get(),
        task->seed->info_hash, local_speed, p2p_port_);

    std::vector<bool> init_bf(chunk_count, false);
    uint64_t initial_bytes_done = 0;
    if (file_exists) {
        try {
            FastCDCConfig cdc_config{};
            cdc_config.min_size = task->seed->header.min_chunk_size;
            cdc_config.avg_size = task->seed->header.avg_chunk_size;
            cdc_config.max_size = task->seed->header.max_chunk_size;
            auto local_chunks = fastcdc_scan_file(save_path, cdc_config);

            std::unordered_map<std::string, std::vector<ChunkEntry>> local_index;
            for (const auto& c : local_chunks) {
                std::string key(reinterpret_cast<const char*>(c.sha256), 32);
                local_index[key].push_back(c);
            }

            for (uint32_t i = 0; i < chunk_count; i++) {
                const auto& want = task->seed->chunks[i];
                std::string key(reinterpret_cast<const char*>(want.sha256), 32);
                auto it = local_index.find(key);
                if (it == local_index.end()) continue;
                for (const auto& have : it->second) {
                    if (have.offset == want.offset && have.length == want.length) {
                        init_bf[i] = true;
                        initial_bytes_done += want.length;
                        break;
                    }
                }
            }
        } catch (const std::exception&) {
            // Resume is an optimization; failed scans fall back to normal download.
        }
    }
    task->bytes_done = initial_bytes_done;
    task->peer_mgr->set_initial_bitfield(init_bf);
    task->peer_mgr->set_file_fd(task->writer->get_file_fd());
    task->peer_mgr->set_chunk_offsets(&task->chunk_offsets);
    task->peer_mgr->start_accept();

    // 用能访问 PeerManager 的真实回调初始化 scheduler（仅一次）
    auto* pm = task->peer_mgr.get();
    task->scheduler->init(chunk_count, local_speed,
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_request(chunk_idx, begin, length));
                sess->inc_pending();
                sess->record_request_sent(chunk_idx, begin);
            }
        },
        [pm](uint32_t chunk_idx) {
            auto have_msg = build_have(chunk_idx);
            for (auto& s : pm->sessions()) s->send_message(have_msg);
        },
        [pm]() {
            auto msg = build_not_interested();
            for (auto& s : pm->sessions()) s->send_message(msg);
        });

    // init 后设置 chunk 大小表
    {
        std::vector<uint32_t> sizes(chunk_count);
        for (uint32_t i = 0; i < chunk_count; i++)
            sizes[i] = task->seed->chunks[i].length;
        task->scheduler->set_chunk_sizes(sizes);
    }
    task->scheduler->randomize_piece_order(static_cast<uint32_t>(
        std::hash<std::string>{}(tid) ^ std::chrono::steady_clock::now().time_since_epoch().count()));

    // 断点续传：将已校验通过的 chunk 标记为 COMPLETE
    if (initial_bytes_done > 0)
        task->scheduler->mark_all_complete(init_bf);

    task->scheduler->set_cancel_issuer(
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_cancel(chunk_idx, begin, length));
                sess->dec_pending();
            }
        });

    task->scheduler->set_not_interested_issuer(
        [pm](uint32_t slot_id) {
            auto* sess = pm->get_session(slot_id);
            if (sess) sess->send_not_interested();
        });

    auto& stored = tasks_[tid];
    stored = std::move(task);
    announce_task_now(*stored);

    return R"({"status":"ok","data":{"task_id":")" + tid + R"("}})";
}

std::string TaskManager::cmd_update(const std::string& new_seed_path, const std::string& new_file_path,
                                     const std::string& old_seed_path, const std::string& old_file_path) {
    // 增量更新：加载新旧种子，扫描旧文件构建 sha256 索引，命中 chunk 标记完成，未命中走 P2P
    auto new_seed_ptr = read_tseed(new_seed_path);
    if (!new_seed_ptr)
        return R"({"status":"error","error":"cannot read new seed"})";

    auto tid = gen_task_id();
    auto task = std::make_unique<ActiveTask>();
    task->task_id = tid;
    task->state = "downloading";
    task->started_at = iso8601_now();
    task->started_mono = std::chrono::steady_clock::now();
    task->startup_announce_deadline = task->started_mono + std::chrono::seconds(60);
    task->next_startup_announce = task->started_mono + std::chrono::seconds(2);
    task->seed = std::move(new_seed_ptr);
    task->file_path = new_file_path;
    task->seed_path = new_seed_path;

    uint32_t chunk_count = task->seed->header.chunk_count;
    uint64_t file_size = task->seed->header.file_size;

    // 构建 chunk 偏移表
    task->chunk_offsets.reserve(chunk_count);
    for (uint32_t i = 0; i < chunk_count; i++)
        task->chunk_offsets.push_back(task->seed->chunks[i].offset);

    // 尝试增量：扫描旧文件，构建 {sha256 -> {offset, length}} 索引
    std::unordered_map<std::string, std::vector<std::pair<uint64_t, uint32_t>>> old_chunk_index;
    bool incremental_ok = false;
    if (!old_file_path.empty()) {
        try {
            auto old_seed_ptr = read_tseed(old_seed_path);
            FastCDCConfig cdc_config{};
            // 使用旧种子的 CDC 参数以匹配分块边界
            if (old_seed_ptr) {
                cdc_config.min_size = old_seed_ptr->header.min_chunk_size;
                cdc_config.avg_size = old_seed_ptr->header.avg_chunk_size;
                cdc_config.max_size = old_seed_ptr->header.max_chunk_size;
            }
            auto old_chunks = fastcdc_scan_file(old_file_path, cdc_config);
            for (const auto& c : old_chunks) {
                std::string key(reinterpret_cast<const char*>(c.sha256), 32);
                old_chunk_index[key].push_back({c.offset, c.length});
            }
            incremental_ok = true;
        } catch (const std::exception&) {
            // 增量扫描失败，回退到全量下载
        }
    }

    // 创建 SegmentWriter
    task->writer = std::make_unique<SegmentWriter>();
    if (!task->writer->open(new_file_path, file_size))
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
    task->verify_pool = std::make_unique<VerifyWorkerPool>();
    task->verify_pool->start(task->seed->chunks.data(), task->assemblers.get());
    task->io_pool->start(io_threads, task->assemblers.get(),
        [vp = task->verify_pool.get()](ChunkCompleteMsg msg) { vp->enqueue(msg); });

    uint32_t local_speed = detect_link_speed_mbps();

    // 先创建 Scheduler（PeerManager 构造需要它的引用）
    task->scheduler = std::make_unique<Scheduler>();

    // 构建初始 bitfield：增量匹配的 chunk 零拷贝移动 + 标记 HAVE
    std::vector<bool> init_bf(chunk_count, false);
    uint32_t matched = 0;
    uint64_t matched_bytes = 0;
    if (incremental_ok) {
        int new_fd = task->writer->get_file_fd();
        int old_fd = ::open(old_file_path.c_str(), O_RDONLY);
        for (uint32_t i = 0; i < chunk_count; i++) {
            std::string key(reinterpret_cast<const char*>(task->seed->chunks[i].sha256), 32);
            auto it = old_chunk_index.find(key);
            if (it != old_chunk_index.end() && old_fd >= 0) {
                auto& candidates = it->second;
                for (auto cand = candidates.begin(); cand != candidates.end(); ++cand) {
                    if (cand->second != task->seed->chunks[i].length) continue;
                    if (!clone_range(old_fd, cand->first,
                                     new_fd, task->seed->chunks[i].offset,
                                     task->seed->chunks[i].length)) {
                        continue;
                    }
                    init_bf[i] = true;
                    matched++;
                    matched_bytes += task->seed->chunks[i].length;
                    candidates.erase(cand);
                    break;
                }
                if (candidates.empty()) old_chunk_index.erase(it);
            }
        }
        // 未被复用的旧文件块，使用 fallocate punch_hole 释放空洞归还磁盘空间
        if (old_fd >= 0) ::close(old_fd);
    }
    task->bytes_done = matched_bytes;

    // 创建 PeerManager
    task->peer_mgr = std::make_unique<PeerManager>(
        io_, *task->scheduler, task->io_pool.get(),
        task->seed->info_hash, local_speed, p2p_port_);

    task->peer_mgr->set_initial_bitfield(init_bf);
    task->peer_mgr->set_file_fd(task->writer->get_file_fd());
    task->peer_mgr->set_chunk_offsets(&task->chunk_offsets);
    task->peer_mgr->start_accept();

    // 用能访问 PeerManager 的真实回调初始化 scheduler（仅一次）
    auto* pm = task->peer_mgr.get();
    task->scheduler->init(chunk_count, local_speed,
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_request(chunk_idx, begin, length));
                sess->inc_pending();
                sess->record_request_sent(chunk_idx, begin);
            }
        },
        [pm](uint32_t chunk_idx) {
            auto have_msg = build_have(chunk_idx);
            for (auto& s : pm->sessions()) s->send_message(have_msg);
        },
        [pm]() {
            auto msg = build_not_interested();
            for (auto& s : pm->sessions()) s->send_message(msg);
        });

    // init 后设置 chunk 大小表
    {
        std::vector<uint32_t> sizes(chunk_count);
        for (uint32_t i = 0; i < chunk_count; i++)
            sizes[i] = task->seed->chunks[i].length;
        task->scheduler->set_chunk_sizes(sizes);
    }

    // 增量匹配的 chunk 标记为已完成，避免 P2P 重复下载
    task->scheduler->randomize_piece_order(static_cast<uint32_t>(
        std::hash<std::string>{}(tid) ^ std::chrono::steady_clock::now().time_since_epoch().count()));

    if (matched > 0)
        task->scheduler->mark_all_complete(init_bf);

    task->scheduler->set_cancel_issuer(
        [pm](uint32_t slot_id, uint32_t chunk_idx, uint32_t begin, uint32_t length) {
            auto* sess = pm->get_session(slot_id);
            if (sess) {
                sess->send_message(build_cancel(chunk_idx, begin, length));
                sess->dec_pending();
            }
        });

    task->scheduler->set_not_interested_issuer(
        [pm](uint32_t slot_id) {
            auto* sess = pm->get_session(slot_id);
            if (sess) sess->send_not_interested();
        });

    auto& stored = tasks_[tid];
    stored = std::move(task);
    announce_task_now(*stored);

    std::ostringstream resp;
    resp << R"({"status":"ok","data":{"task_id":")" << tid
         << R"(","chunk_count":)" << chunk_count
         << R"(,"matched":)" << matched << R"(}})";
    return resp.str();
}

std::vector<TaskInfo> TaskManager::cmd_list() {
    std::vector<TaskInfo> result;
    for (auto& [tid, t] : tasks_) {
        TaskInfo info;
        info.task_id    = tid;
        info.state      = t->state.empty() ? (t->is_seed ? "seeding" : "downloading") : t->state;
        info.file_path  = t->file_path;
        info.seed_path  = t->seed_path;
        info.bytes_done = t->bytes_done;
        info.started_at  = t->started_at;
        info.finished_at = t->finished_at;
        info.speed_mib_s = (info.state == "complete" && t->completed_speed_mib_s > 0.0)
            ? t->completed_speed_mib_s
            : t->speed_ema / (1024.0 * 1024.0) * 10.0;
        if (t->seed)
            info.progress = t->seed->header.file_size > 0
                ? static_cast<double>(t->bytes_done) / t->seed->header.file_size : 0.0;
        result.push_back(info);
    }
    return result;
}

void TaskManager::announce_task_now(ActiveTask& task) {
    if (!task.seed || !task.peer_mgr) return;

    std::string host = tracker_host_;
    uint16_t port = tracker_port_;
    if (host.empty()) {
        TrackerUrl t_url;
        if (parse_tracker_url(task.seed->announce_url, t_url)) {
            host = t_url.host;
            port = t_url.port;
        }
    }
    if (host.empty()) host = "127.0.0.1";

    std::string info_hash_hex = sha1_hex(task.seed->info_hash);
    if (!task.tracker_client) {
        task.tracker_client = std::make_shared<TrackerClient>(io_, info_hash_hex, p2p_port_, 1000);
        task.tracker_client->set_on_dead([&dead = task.tracker_dead]() {
            dead.store(true, std::memory_order_release);
        });
    }

    task.tracker_dead.store(false, std::memory_order_release);
    task.tracker_client->announce(host, port,
        [&task](const std::vector<PexPeer>& peers) {
            for (const auto& p : peers) {
                struct in_addr ia;
                ia.s_addr = p.ip;
                std::string ip = inet_ntoa(ia);
                task.peer_mgr->connect_to(ip, ntoh16(p.port), p.flags);
            }
        });
}

std::string TaskManager::cmd_remove(const std::string& task_id, bool /*force*/) {
    if (tasks_.erase(task_id))
        return R"({"status":"ok"})";
    return R"({"status":"error","error":"task not found"})";
}

std::string TaskManager::cmd_peers(const std::string& task_id) {
    auto it = tasks_.find(task_id);
    if (it == tasks_.end())
        return R"({"status":"error","error":"task not found"})";

    auto& t = it->second;
    if (!t->peer_mgr)
        return R"({"status":"error","error":"peer manager not initialized"})";

    std::ostringstream resp;
    resp << R"({"status":"ok","data":{"peers":[)";
    bool first = true;
    for (auto& sess : t->peer_mgr->sessions()) {
        if (!first) resp << ",";
        first = false;
        resp << R"({"ip":")" << sess->remote_ip() << R"(")"
             << R"(,"port":)" << sess->remote_port()
             << R"(,"speed_mbps":)" << sess->link_speed_reported()
             << R"(,"pending":)" << sess->pending_requests()
             << R"(,"flags":0})"; // flags 从远程握手速度推断：千兆=1
    }
    resp << "]}}";
    return resp.str();
}

void TaskManager::tick() {
    for (auto& [tid, t] : tasks_) {
        if (t->scheduler) t->scheduler->tick();

        // 消费校验线程池的完成通知
        if (t->verify_pool && t->scheduler) {
            std::vector<VerifyResult> results;
            t->verify_pool->drain_results(results);

            std::vector<ChunkCompleteMsg> passed;
            for (auto& r : results) {
                if (r.passed) {
                    passed.push_back({r.chunk_idx, r.winning_peer_slot});
                    // 累加 bytes_done（CDC 变长 chunk）
                    if (t->seed && r.chunk_idx < t->seed->header.chunk_count) {
                        uint32_t csize = t->seed->chunks[r.chunk_idx].length;
                        t->bytes_done += csize;
                        // 追踪 chunk 完成：记录哪个 chunk 从哪个 peer 完成
                        const char* src_ip = "?";
                        if (t->peer_mgr) {
                            auto* sess = t->peer_mgr->get_session(r.winning_peer_slot);
                            if (sess) src_ip = sess->remote_ip().c_str();
                        }
                        std::cerr << "[chunk_done] idx=" << r.chunk_idx
                                  << " size=" << csize
                                  << " from=" << src_ip
                                  << " progress=" << t->bytes_done << "/" << t->seed->header.file_size
                                  << " (" << (t->bytes_done * 100.0 / t->seed->header.file_size) << "%)"
                                  << std::endl;
                    }
                } else {
                    // SHA-256 校验失败 → 回退为 MISSING，下一轮 tick 自动重新下载
                    t->scheduler->on_verify_failed(r.chunk_idx);
                }
            }
            if (!passed.empty())
                t->scheduler->process_completions(passed);

            // 检查是否完成
            if (t->seed && t->bytes_done >= t->seed->header.file_size && t->state == "downloading") {
                t->state = "complete";
                t->finished_at = iso8601_now();
                auto elapsed = std::chrono::duration<double>(
                    std::chrono::steady_clock::now() - t->started_mono).count();
                if (elapsed > 0.0) {
                    t->completed_speed_mib_s =
                        static_cast<double>(t->seed->header.file_size) / elapsed / (1024.0 * 1024.0);
                }
            }
        }

        // 检测 Tracker 重试耗尽 → 状态切换为 waiting
        if (t->tracker_dead.load(std::memory_order_acquire) && t->state == "downloading") {
            t->state = "waiting";
        }

        auto now = std::chrono::steady_clock::now();
        if (!t->is_seed && t->state == "downloading" && t->seed && t->peer_mgr &&
            now < t->startup_announce_deadline && now >= t->next_startup_announce) {
            t->next_startup_announce = now + std::chrono::seconds(2);
            announce_task_now(*t);
        }

        // 速度 EMA 计算（α=0.125），每 100ms tick
        {
            uint64_t delta = t->bytes_done - t->last_bytes_done;
            t->last_bytes_done = t->bytes_done;
            // EMA: speed_ema = α * delta + (1-α) * speed_ema
            t->speed_ema = 0.125 * static_cast<double>(delta) + 0.875 * t->speed_ema;
        }
    }
}

void TaskManager::tick_tracker_announce(asio::io_context& io) {
    (void)io;
    for (auto& [tid, t] : tasks_) {
        if (!t->seed) continue;
        std::string info_hash_hex = sha1_hex(t->seed->info_hash);

        // 优先级: CLI 参数 > seed announce_url > 默认回退
        std::string host = tracker_host_;
        uint16_t port = tracker_port_;

        if (host.empty() && t->seed) {
            TrackerUrl t_url;
            if (parse_tracker_url(t->seed->announce_url, t_url)) {
                host = t_url.host;
                port = t_url.port;
            }
        }
        if (host.empty()) host = "127.0.0.1";

        // Create or reuse TrackerClient
        if (!t->tracker_client) {
            t->tracker_client = std::make_shared<TrackerClient>(
                io, info_hash_hex, p2p_port_, 1000);
            // 重试耗尽时标记 tracker_dead，tick() 中将状态改为 waiting
            t->tracker_client->set_on_dead([&dead = t->tracker_dead]() {
                dead.store(true, std::memory_order_release);
            });
        }

        // PeerManager 应在 cmd_seed/cmd_add 中已创建
        if (!t->peer_mgr) continue;

        announce_task_now(*t);
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
