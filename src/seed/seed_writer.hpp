#ifndef THINBT_SEED_WRITER_HPP
#define THINBT_SEED_WRITER_HPP

#include "tseed.hpp"
#include <string>
#include <vector>

namespace thinbt {

void write_tseed(const std::string& output_path,
                 const std::string& file_path,
                 const std::string& file_name,
                 const std::string& announce_url,
                 const std::vector<ChunkEntry>& chunks,
                 uint32_t min_chunk_size,
                 uint32_t avg_chunk_size,
                 uint32_t max_chunk_size);

} // namespace thinbt

#endif
