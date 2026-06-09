#ifndef THINBT_IPC_SERVER_HPP
#define THINBT_IPC_SERVER_HPP

#include "common/platform.hpp"
#include <string>
#include <memory>
#include <cstdint>

namespace thinbt {

class TaskManager;

class IpcServer {
public:
    IpcServer(TaskManager& task_mgr, uint16_t port = 16888);

    void handle(const std::string& request, std::string& response);

private:
    TaskManager& task_mgr_;
    uint16_t port_;
};

} // namespace thinbt
#endif
