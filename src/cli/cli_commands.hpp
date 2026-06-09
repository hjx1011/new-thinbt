#ifndef THINBT_CLI_COMMANDS_HPP
#define THINBT_CLI_COMMANDS_HPP

#include <string>
#include <cstdint>

namespace thinbt {

std::string send_ipc(const std::string& json, uint16_t port = 16888);
std::string cli_make(int argc, char* argv[]);
std::string cli_info(int argc, char* argv[]);
std::string cli_update(int argc, char* argv[], uint16_t ipc_port = 16888);

} // namespace thinbt

#endif
