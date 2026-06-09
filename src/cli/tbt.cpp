#include "cli/cli_commands.hpp"

#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

int main(int argc, char* argv[]) {
    uint16_t ipc_port = 16888;

    // 解析全局 --ipc-port 选项（在子命令之前）
    int first_cmd = 1;
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--ipc-port") == 0 && i + 1 < argc) {
            ipc_port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else {
            first_cmd = i;
            break;
        }
    }

    if (first_cmd >= argc) {
        std::cerr << "Usage: tbt [--ipc-port <port>] <make|info|seed|add|update|list|peers|remove> [args...]\n"
                  << "  tbt update <new.tseed> <new_file> <old.tseed> <old_file>\n";
        return 1;
    }

    std::string cmd = argv[first_cmd];
    // 将子命令参数偏移到 argv[1] 位置，供 cli_make/cli_info/cli_update 使用
    int sub_argc = argc - first_cmd + 1;
    char** sub_argv = argv + first_cmd - 1;

    if (cmd == "make") {
        std::cout << thinbt::cli_make(sub_argc, sub_argv) << std::endl;
    } else if (cmd == "info" && sub_argc >= 3) {
        std::cout << thinbt::cli_info(sub_argc, sub_argv) << std::endl;
    } else if (cmd == "add" && sub_argc >= 3) {
        std::string req = R"({"cmd":"add","args":{"seed_path":")"
            + std::string(sub_argv[2]) + R"(","save_path":")"
            + (sub_argc >= 4 ? sub_argv[3] : "downloaded_file") + R"("}})";
        std::cout << thinbt::send_ipc(req, ipc_port) << std::endl;
    } else if (cmd == "seed" && sub_argc >= 4) {
        std::string req = R"({"cmd":"seed","args":{"seed_path":")"
            + std::string(sub_argv[2]) + R"(","file_path":")"
            + std::string(sub_argv[3]) + R"("}})";
        std::cout << thinbt::send_ipc(req, ipc_port) << std::endl;
    } else if (cmd == "list") {
        std::cout << thinbt::send_ipc(R"({"cmd":"list","args":{}})", ipc_port) << std::endl;
    } else if (cmd == "update" && sub_argc >= 6) {
        std::cout << thinbt::cli_update(sub_argc, sub_argv, ipc_port) << std::endl;
    } else if (cmd == "update" && sub_argc >= 3) {
        std::string req = R"({"cmd":"update","args":{"task_id":")"
            + std::string(sub_argv[2]) + R"("}})";
        std::cout << thinbt::send_ipc(req, ipc_port) << std::endl;
    } else if (cmd == "peers" && sub_argc >= 3) {
        std::string req = R"({"cmd":"peers","args":{"task_id":")"
            + std::string(sub_argv[2]) + R"("}})";
        std::cout << thinbt::send_ipc(req, ipc_port) << std::endl;
    } else if (cmd == "remove" && sub_argc >= 3) {
        std::string req = R"({"cmd":"remove","args":{"task_id":")"
            + std::string(sub_argv[2]) + R"("}})";
        std::cout << thinbt::send_ipc(req, ipc_port) << std::endl;
    } else {
        std::cout << R"({"status":"error","error":"usage"})" << std::endl;
    }
    return 0;
}
