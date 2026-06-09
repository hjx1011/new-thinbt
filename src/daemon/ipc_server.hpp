#ifndef THINBT_IPC_SERVER_HPP
#define THINBT_IPC_SERVER_HPP

#include "common/platform.hpp"
#include <asio.hpp>
#include <string>
#include <memory>
#include <cstdint>

namespace thinbt {

class TaskManager;

class IpcServer {
public:
    IpcServer(asio::io_context& io, TaskManager& task_mgr, uint16_t port = 16888);

    void start();
    void handle(const std::string& request, std::string& response);

private:
    void do_accept();
    void handle_client(std::shared_ptr<asio::ip::tcp::socket> socket);

    asio::io_context& io_;
    TaskManager& task_mgr_;
    uint16_t port_;
    asio::ip::tcp::acceptor acceptor_;
};

} // namespace thinbt
#endif
