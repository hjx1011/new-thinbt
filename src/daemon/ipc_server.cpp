#include "ipc_server.hpp"
#include "task_manager.hpp"
#include <sstream>
#include <iostream>

namespace thinbt {

IpcServer::IpcServer(asio::io_context& io, TaskManager& task_mgr, uint16_t port)
    : io_(io), task_mgr_(task_mgr), port_(port),
      acceptor_(io, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), port)) {}

void IpcServer::start() {
    do_accept();
    std::cout << "IPC listening on 127.0.0.1:" << port_ << std::endl;
}

void IpcServer::do_accept() {
    auto socket = std::make_shared<asio::ip::tcp::socket>(io_);
    acceptor_.async_accept(*socket, [this, socket](asio::error_code ec) {
        if (!ec) handle_client(socket);
        do_accept();
    });
}

void IpcServer::handle_client(std::shared_ptr<asio::ip::tcp::socket> socket) {
    auto buf = std::make_shared<std::vector<uint8_t>>(65536);
    socket->async_read_some(asio::buffer(*buf),
        [this, socket, buf](asio::error_code ec, size_t len) {
            if (ec || len == 0) return;
            std::string request(reinterpret_cast<char*>(buf->data()), len);
            std::string response;
            handle(request, response);
            response += "\n";
            auto resp_ptr = std::make_shared<std::string>(std::move(response));
            asio::async_write(*socket, asio::buffer(*resp_ptr),
                [socket, resp_ptr](asio::error_code, size_t) {});
        });
}

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
        std::string seed_path, file_path;
        auto sp = request.find("\"seed_path\":\"");
        if (sp != std::string::npos) {
            auto ss = sp + 13;
            auto se = request.find('"', ss);
            if (se != std::string::npos) seed_path = request.substr(ss, se - ss);
        }
        auto fp = request.find("\"file_path\":\"");
        if (fp != std::string::npos) {
            auto fs = fp + 13;
            auto fe = request.find('"', fs);
            if (fe != std::string::npos) file_path = request.substr(fs, fe - fs);
        }
        response = task_mgr_.cmd_seed(seed_path, file_path);
    } else if (cmd == "add") {
        std::string seed_path, save_path;
        auto sp = request.find("\"seed_path\":\"");
        if (sp != std::string::npos) {
            auto ss = sp + 13;
            auto se = request.find('"', ss);
            if (se != std::string::npos) seed_path = request.substr(ss, se - ss);
        }
        if (seed_path.empty()) {
            response = R"({"status":"error","error":"missing seed_path"})";
        } else {
            auto fp = request.find("\"save_path\":\"");
            if (fp != std::string::npos) {
                auto fs = fp + 13;
                auto fe = request.find('"', fs);
                if (fe != std::string::npos) save_path = request.substr(fs, fe - fs);
            }
            if (save_path.empty()) save_path = "downloaded_file";
            response = task_mgr_.cmd_add(seed_path, save_path);
        }
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
                << R"(,"speed_mib_s":)" << tasks[i].speed_mib_s
                << R"(,"file_path":")" << tasks[i].file_path << R"(")"
                << R"(,"seed_path":")" << tasks[i].seed_path << R"("})";
        }
        oss << "]}}";
        response = oss.str();
    } else if (cmd == "remove") {
        std::string task_id;
        auto tp = request.find("\"task_id\":\"");
        if (tp != std::string::npos) {
            auto ts = tp + 11;
            auto te = request.find('"', ts);
            if (te != std::string::npos) task_id = request.substr(ts, te - ts);
        }
        response = task_mgr_.cmd_remove(task_id, false);
    } else if (cmd == "update") {
        auto extract = [&](const std::string& key) -> std::string {
            auto kp = request.find("\"" + key + "\":\"");
            if (kp == std::string::npos) return {};
            auto vs = kp + key.size() + 4;
            auto ve = request.find('"', vs);
            if (ve == std::string::npos) return {};
            return request.substr(vs, ve - vs);
        };
        std::string new_seed = extract("new_seed");
        std::string task_id   = extract("task_id");

        if (!new_seed.empty()) {
            // 4-arg form: incremental update
            std::string new_file = extract("new_file");
            std::string old_seed = extract("old_seed");
            std::string old_file = extract("old_file");
            response = R"({"status":"error","error":"incremental update not yet implemented"})";
        } else if (!task_id.empty()) {
            // 1-arg form: task status query
            auto tasks = task_mgr_.cmd_list();
            bool found = false;
            for (const auto& t : tasks) {
                if (t.task_id == task_id) {
                    std::ostringstream oss;
                    oss << R"({"status":"ok","data":{)";
                    oss << R"("task_id":")" << t.task_id << R"(")"
                        << R"(,"state":")" << t.state << R"(")"
                        << R"(,"progress":)" << t.progress
                        << R"(,"speed_mib_s":)" << t.speed_mib_s
                        << R"(,"bytes_done":)" << t.bytes_done
                        << R"(,"file_path":")" << t.file_path << R"(")"
                        << R"(,"seed_path":")" << t.seed_path << R"("}})";
                    response = oss.str();
                    found = true;
                    break;
                }
            }
            if (!found) {
                response = R"({"status":"error","error":"task not found"})";
            }
        } else {
            response = R"({"status":"error","error":"update requires task_id or new_seed"})";
        }
    } else if (cmd == "peers") {
        std::string task_id;
        auto tp = request.find("\"task_id\":\"");
        if (tp != std::string::npos) {
            auto ts = tp + 11;
            auto te = request.find('"', ts);
            if (te != std::string::npos) task_id = request.substr(ts, te - ts);
        }
        if (task_id.empty()) {
            response = R"({"status":"error","error":"missing task_id"})";
        } else {
            response = task_mgr_.cmd_peers(task_id);
        }
    } else {
        response = R"({"status":"error","error":"unknown command"})";
    }
}

} // namespace thinbt
