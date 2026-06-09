#include "task_manager.hpp"
#include "ipc_server.hpp"
#include "peer_manager.hpp"
#include "tracker_client.hpp"
#include "tracker_acceptor.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "common/hash.hpp"
#include "common/net_util.hpp"
#include <asio.hpp>
#include <iostream>
#include <csignal>
#include <memory>
#include <thread>
#include <map>

static std::atomic<bool> running{true};

int main(int argc, char* argv[]) {
    using thinbt::TaskManager;
    using thinbt::IpcServer;
    using thinbt::TrackerAcceptor;

    uint16_t ipc_port     = 16888;
    uint16_t tracker_port = 8080;
    uint16_t p2p_port     = 16889;
    std::string tracker_host = "";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--ipc-port" && i + 1 < argc)      ipc_port     = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (arg == "--tracker-host" && i + 1 < argc) tracker_host = argv[++i];
        else if (arg == "--tracker-port" && i + 1 < argc) tracker_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (arg == "--p2p-port" && i + 1 < argc)     p2p_port     = static_cast<uint16_t>(std::stoi(argv[++i]));
    }

    std::cout << "thinbtd v1.0.0 (Asio)\n"
              << "  IPC: " << ipc_port
              << "  Tracker: " << tracker_port
              << "  P2P: " << p2p_port << std::endl;

    asio::io_context ioc;

    TaskManager task_mgr(ioc, p2p_port, tracker_host, tracker_port);
    IpcServer ipc(ioc, task_mgr, ipc_port);
    ipc.start();

    // 内置 Tracker 监听所有网卡（供其他节点 announce）
    TrackerAcceptor tracker_acceptor(ioc, task_mgr.tracker(), tracker_port);
    tracker_acceptor.start();

    signal(SIGINT, [](int) { running.store(false); });
    signal(SIGTERM, [](int) { running.store(false); });

    // ── Heartbeat timer (100ms) ──
    asio::steady_timer heartbeat(ioc, std::chrono::milliseconds(100));
    uint64_t tick_count = 0;

    // ── Heartbeat tick ──
    std::function<void(const asio::error_code&)> tick = [&](const asio::error_code& ec) {
        if (ec || !running.load()) return;
        tick_count++;

        // Scheduler tick (100ms)
        task_mgr.tick();

        // Choke evaluation (10s)
        if (tick_count % 100 == 0) {
            task_mgr.tick_choke_all();
        }

        // Tracker stale cleanup (6s)
        if (tick_count % 60 == 0) {
            task_mgr.tracker().cleanup_stale(90);
        }

        // Tracker announce (30s)
        if (tick_count % 300 == 0) {
            task_mgr.tick_tracker_announce(ioc);
        }

        // PEX Delta Gossip (60s)
        if (tick_count % 600 == 0) {
            task_mgr.tick_pex_all();
        }

        heartbeat.expires_after(std::chrono::milliseconds(100));
        heartbeat.async_wait(tick);
    };
    heartbeat.async_wait(tick);

    std::cout << "Event loop running..." << std::endl;
    ioc.run();

    std::cout << "thinbtd stopped." << std::endl;
    return 0;
}
