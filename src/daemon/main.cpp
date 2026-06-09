#include "task_manager.hpp"
#include "ipc_server.hpp"
#include <iostream>
#include <csignal>
#include <thread>
#include <atomic>

using namespace thinbt;

static std::atomic<bool> running{true};

int main(int argc, char* argv[]) {
    uint16_t ipc_port     = 16888;
    uint16_t tracker_port = 8080;
    uint16_t p2p_port     = 16889;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--ipc-port" && i + 1 < argc)      ipc_port     = std::stoi(argv[++i]);
        else if (arg == "--tracker-port" && i + 1 < argc) tracker_port = std::stoi(argv[++i]);
        else if (arg == "--p2p-port" && i + 1 < argc)     p2p_port     = std::stoi(argv[++i]);
    }

    std::cout << "thinbtd v1.0.0\n  IPC: " << ipc_port
              << "  Tracker: " << tracker_port
              << "  P2P: " << p2p_port << std::endl;

    TaskManager task_mgr(p2p_port);
    IpcServer ipc(task_mgr, ipc_port);

    signal(SIGINT, [](int) { running.store(false); });
    signal(SIGTERM, [](int) { running.store(false); });

    // Simple TCP accept loop for IPC
    // Full Asio integration in future iteration
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(ipc_port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, 5);

    std::cout << "IPC listening on 127.0.0.1:" << ipc_port << std::endl;

    while (running.load()) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(listen_fd, &rfds);
        struct timeval tv{0, 100000}; // 100ms timeout
        if (select(listen_fd + 1, &rfds, nullptr, nullptr, &tv) > 0) {
            int client = accept(listen_fd, nullptr, nullptr);
            if (client >= 0) {
                char buf[65536] = {};
                ssize_t n = recv(client, buf, sizeof(buf) - 1, 0);
                if (n > 0) {
                    std::string request(buf, n);
                    std::string response;
                    ipc.handle(request, response);
                    response += "\n";
                    send(client, response.c_str(), response.size(), 0);
                }
                close(client);
            }
        }
        task_mgr.tick();
    }

    close(listen_fd);
    std::cout << "thinbtd stopped." << std::endl;
    return 0;
}
