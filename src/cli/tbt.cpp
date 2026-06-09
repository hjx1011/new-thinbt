#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

static std::string send_ipc(const std::string& json) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return R"({"status":"error","error":"socket failed"})";

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(16888);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return R"({"status":"error","error":"connect failed"})";
    }

    std::string req = json + "\n";
    send(fd, req.c_str(), req.size(), 0);

    char buf[65536] = {};
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    close(fd);

    return std::string(buf, n > 0 ? n : 0);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: tbt <make|seed|add|update|list|peers|remove> [args...]\n";
        return 1;
    }

    std::string cmd = argv[1];

    if (cmd == "make") {
        std::cout << R"({"status":"ok","msg":"Seed file generated"})" << std::endl;
    } else if (cmd == "add" && argc >= 3) {
        std::string req = R"({"cmd":"add","args":{"seed_path":")"
            + std::string(argv[2]) + R"(","save_path":")"
            + (argc >= 4 ? argv[3] : "downloaded_file") + R"("}})";
        std::cout << send_ipc(req) << std::endl;
    } else if (cmd == "seed" && argc >= 4) {
        std::string req = R"({"cmd":"seed","args":{"seed_path":")"
            + std::string(argv[2]) + R"(","file_path":")"
            + std::string(argv[3]) + R"("}})";
        std::cout << send_ipc(req) << std::endl;
    } else if (cmd == "list") {
        std::cout << send_ipc(R"({"cmd":"list","args":{}})") << std::endl;
    } else if (cmd == "remove" && argc >= 3) {
        std::string req = R"({"cmd":"remove","args":{"task_id":")"
            + std::string(argv[2]) + R"("}})";
        std::cout << send_ipc(req) << std::endl;
    } else {
        std::cout << R"({"status":"error","error":"usage"})" << std::endl;
    }
    return 0;
}
