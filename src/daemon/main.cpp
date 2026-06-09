#include "task_manager.hpp"
#include "ipc_server.hpp"
#include "peer_manager.hpp"
#include "tracker_client.hpp"
#include "scheduler.hpp"
#include "io_worker.hpp"
#include "common/hash.hpp"
#include "common/net_util.hpp"
#include <asio.hpp>
#include <iostream>
#include <csignal>
#include <memory>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <map>

using namespace thinbt;

static std::atomic<bool> running{true};

int main(int argc, char* argv[]) {
    uint16_t ipc_port     = 16888;
    uint16_t tracker_port = 8080;
    uint16_t p2p_port     = 16889;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--ipc-port" && i + 1 < argc)      ipc_port     = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (arg == "--tracker-port" && i + 1 < argc) tracker_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (arg == "--p2p-port" && i + 1 < argc)     p2p_port     = static_cast<uint16_t>(std::stoi(argv[++i]));
    }

    std::cout << "thinbtd v1.0.0 (Asio)\n"
              << "  IPC: " << ipc_port
              << "  Tracker: " << tracker_port
              << "  P2P: " << p2p_port << std::endl;

    asio::io_context ioc;

    TaskManager task_mgr(p2p_port);
    IpcServer ipc(task_mgr, ipc_port);

    signal(SIGINT, [](int) { running.store(false); });
    signal(SIGTERM, [](int) { running.store(false); });

    // ── IPC TCP listener ──
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(ipc_port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, 5);
    std::cout << "IPC listening on 127.0.0.1:" << ipc_port << std::endl;

    // ── Heartbeat timer (100ms) ──
    asio::steady_timer heartbeat(ioc, std::chrono::milliseconds(100));
    uint64_t tick_count = 0;

    // Global Scheduler (shared across tasks)
    auto global_sched = std::make_unique<Scheduler>();
    global_sched->init(0, 1000,
        [](uint32_t, uint32_t, uint32_t, uint32_t){},
        [](uint32_t){});

    // ── Heartbeat tick ──
    std::function<void(const asio::error_code&)> tick = [&](const asio::error_code& ec) {
        if (ec || !running.load()) return;
        tick_count++;

        // IPC: check for CLI commands
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(listen_fd, &rfds);
        struct timeval tv{0, 0};
        if (select(listen_fd + 1, &rfds, nullptr, nullptr, &tv) > 0) {
            int client = accept(listen_fd, nullptr, nullptr);
            if (client >= 0) {
                char buf[65536] = {};
                ssize_t n = recv(client, buf, sizeof(buf) - 1, 0);
                if (n > 0) {
                    std::string request(buf, static_cast<size_t>(n));
                    std::string response;
                    ipc.handle(request, response);
                    response += "\n";
                    send(client, response.c_str(), response.size(), 0);
                }
                close(client);
            }
        }

        // Scheduler tick
        task_mgr.tick();

        // Tracker announce (30s)
        if (tick_count % 300 == 0) {
            task_mgr.tick_tracker_announce(ioc);
        }

        // Choke evaluation (10s)
        if (tick_count % 100 == 0) {
            task_mgr.tick_choke_all();
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

    close(listen_fd);
    std::cout << "thinbtd stopped." << std::endl;
    return 0;
}
