#ifndef THINBT_CLI_COMMANDS_HPP
#define THINBT_CLI_COMMANDS_HPP

#include <string>

namespace thinbt {

std::string cli_make(int argc, char* argv[]);
std::string cli_info(int argc, char* argv[]);
std::string cli_update(int argc, char* argv[]);

} // namespace thinbt

#endif
