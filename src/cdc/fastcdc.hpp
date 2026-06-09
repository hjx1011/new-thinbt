#ifndef THINBT_FASTCDC_HPP
#define THINBT_FASTCDC_HPP

#include "common/platform.hpp"
#include "common/hash.hpp"
#include "seed/tseed.hpp"
#include <vector>
#include <functional>
#include <cstdint>

namespace thinbt {

struct FastCDCConfig {
    uint32_t min_size = 16 * 1024;
    uint32_t avg_size = 128 * 1024;
    uint32_t max_size = 1024 * 1024;
};

// Scan a file with FastCDC, returning chunk entries
std::vector<ChunkEntry> fastcdc_scan_file(const std::string& file_path,
                                           const FastCDCConfig& config = {});

// Scan from memory buffer, calling callback for each chunk boundary
using ChunkCallback = std::function<void(uint64_t offset, uint32_t length, const Sha256Digest& hash)>;
void fastcdc_scan_buffer(const uint8_t* data, uint64_t size,
                          const FastCDCConfig& config,
                          ChunkCallback callback);

} // namespace thinbt
#endif
