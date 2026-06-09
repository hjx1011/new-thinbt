#include "tseed.hpp"

#include <fstream>
#include <stdexcept>
#include <cstring>

namespace thinbt {

void write_tseed(const std::string& output_path,
                 const std::string& file_path,
                 const std::string& file_name,
                 const std::string& announce_url,
                 const std::vector<ChunkEntry>& chunks,
                 uint32_t min_chunk_size,
                 uint32_t avg_chunk_size,
                 uint32_t max_chunk_size) {

    // Compute total file size from last chunk
    uint64_t total_file_size = 0;
    for (const auto& c : chunks) {
        uint64_t chunk_end = c.offset + c.length;
        if (chunk_end > total_file_size) total_file_size = chunk_end;
    }

    // Compute SHA-256 of the source file
    auto file_digest = sha256_file(file_path);

    // Build header in host byte order first
    TSeedHeader hdr{};
    hdr.magic          = TSeedHeader::MAGIC;
    hdr.version        = 1;
    hdr.flags          = 0;
    hdr.chunk_count    = static_cast<uint32_t>(chunks.size());
    hdr.file_size      = total_file_size;
    hdr.min_chunk_size = min_chunk_size;
    hdr.avg_chunk_size = avg_chunk_size;
    hdr.max_chunk_size = max_chunk_size;
    hdr.file_name_len  = static_cast<uint32_t>(file_name.size());
    hdr.announce_len   = static_cast<uint32_t>(announce_url.size());
    memcpy(hdr.file_sha256, file_digest.data(), 32);
    memset(hdr.reserved, 0, sizeof(hdr.reserved));

    // Convert to network byte order for writing
    hdr.to_network_endian();

    std::ofstream f(output_path, std::ios::binary);
    if (!f) throw std::runtime_error("Cannot create .tseed: " + output_path);

    // Write header
    f.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));

    // Write file_name
    f.write(file_name.data(), file_name.size());

    // Write announce URL
    f.write(announce_url.data(), announce_url.size());

    // Write chunks in network byte order
    for (auto c : chunks) {
        c.to_network_endian();
        f.write(reinterpret_cast<const char*>(&c), sizeof(ChunkEntry));
    }

    if (!f) throw std::runtime_error("Write failed: " + output_path);
}

} // namespace thinbt
