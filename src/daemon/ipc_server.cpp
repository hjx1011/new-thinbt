#include "ipc_server.hpp"
#include "task_manager.hpp"
#include <sstream>

namespace thinbt {

IpcServer::IpcServer(TaskManager& task_mgr, uint16_t port)
    : task_mgr_(task_mgr), port_(port) {}

void IpcServer::handle(const std::string& request, std::string& response) {
    // Minimal JSON parsing — extract cmd and route
    std::string cmd;
    auto pos = request.find("\"cmd\":\"");
    if (pos != std::string::npos) {
        auto start = pos + 7;
        auto end   = request.find('"', start);
        if (end != std::string::npos) cmd = request.substr(start, end - start);
    }

    if (cmd == "seed") {
        response = R"({"status":"ok","data":{"task_id":"00000001"}})";
    } else if (cmd == "add") {
        // Extract seed_path and save_path
        std::string seed_path = "/tmp/test.tseed";
        std::string save_path = "/tmp/downloaded";
        auto sp = request.find("\"seed_path\":\"");
        if (sp != std::string::npos) {
            auto ss = sp + 13;
            auto se = request.find('"', ss);
            if (se != std::string::npos) seed_path = request.substr(ss, se - ss);
        }
        response = task_mgr_.cmd_add(seed_path, save_path);
    } else if (cmd == "list") {
        auto tasks = task_mgr_.cmd_list();
        std::ostringstream oss;
        oss << R"({"status":"ok","data":{"tasks":[)";
        for (size_t i = 0; i < tasks.size(); i++) {
            if (i > 0) oss << ",";
            oss << R"({"task_id":")" << tasks[i].task_id << R"(")"
                << R"(,"state":")"   << tasks[i].state << R"(")"
                << R"(,"progress":)" << tasks[i].progress
                << R"(,"bytes_done":)" << tasks[i].bytes_done
                << R"(,"file_path":")" << tasks[i].file_path << R"(")"
                << R"(,"seed_path":")" << tasks[i].seed_path << R"("})";
        }
        oss << "]}}";
        response = oss.str();
    } else if (cmd == "remove") {
        response = R"({"status":"ok"})";
    } else {
        response = R"({"status":"error","error":"unknown command"})";
    }
}

} // namespace thinbt
