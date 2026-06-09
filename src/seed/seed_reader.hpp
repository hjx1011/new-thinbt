#ifndef THINBT_SEED_READER_HPP
#define THINBT_SEED_READER_HPP

#include "tseed.hpp"
#include <memory>
#include <string>

namespace thinbt {

std::unique_ptr<TSeedFile> read_tseed(const std::string& path);

} // namespace thinbt
#endif
