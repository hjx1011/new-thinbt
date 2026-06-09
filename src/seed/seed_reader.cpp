#include "tseed.hpp"

#include <fstream>
#include <stdexcept>
#include <cstring>

namespace thinbt {

static void read_exact(std::ifstream& f, void* buf, size_t len) {
    f.read(reinterpret_cast<char*>(buf), static_cast<std::streamsize>(len));
    if (!f || f.gcount() != static_cast<std::streamsize>(len)) {
        throw std::runtime_error("Failed to read .tseed: unexpected EOF");
    }
}

std::unique_ptr<TSeedFile> read_tseed(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("Cannot open .tseed: " + path);

    auto seed = std::make_unique<TSeedFile>();

    // Read header (92 bytes, Big-Endian on disk)
    read_exact(f, &seed->header, sizeof(TSeedHeader));

    // Validate magic: header is in network byte order
    if (seed->header.magic != TSeedHeader::MAGIC) {
        // Try byte-swapped (maybe already in host order from a buggy writer)
        seed->header.to_host_endian();
        if (seed->header.magic != TSeedHeader::MAGIC) {
            throw std::runtime_error("Invalid .tseed magic number: 0x"
                + sha256_hex(sha256(reinterpret_cast<const uint8_t*>(&seed->header), 4)).substr(0, 8));
        }
    } else {
        seed->header.to_host_endian();
    }

    if (seed->header.version != 1) {
        throw std::runtime_error("Unsupported .tseed version: "
            + std::to_string(seed->header.version));
    }

    // Read file_name
    seed->file_name.resize(seed->header.file_name_len);
    read_exact(f, seed->file_name.data(), seed->header.file_name_len);

    // Read announce URL
    seed->announce_url.resize(seed->header.announce_len);
    read_exact(f, seed->announce_url.data(), seed->header.announce_len);

    // Read chunks (each 44 bytes, Big-Endian on disk)
    seed->chunks.resize(seed->header.chunk_count);
    for (uint32_t i = 0; i < seed->header.chunk_count; i++) {
        read_exact(f, &seed->chunks[i], sizeof(ChunkEntry));
        seed->chunks[i].to_host_endian();
    }

    // Compute InfoHash: SHA-1(file_sha256 || ChunkEntry[])
    seed->info_hash = seed->compute_info_hash();

    return seed;
}

Sha1Digest TSeedFile::compute_info_hash() const {
    // InfoHash = SHA-1 of (file_sha256 || all ChunkEntry[] in network byte order)
    // file_sha256 is already raw bytes (not endian-dependent)
    // ChunkEntry[] must be serialized back to Big-Endian for hashing
    std::vector<uint8_t> buf;
    buf.reserve(32 + chunks.size() * 44);

    // 1. file_sha256 (32 bytes, raw)
    buf.insert(buf.end(), header.file_sha256, header.file_sha256 + 32);

    // 2. ChunkEntry[] in network byte order
    for (const auto& c : chunks) {
        uint64_t off_be = hton64(c.offset);
        uint32_t len_be = hton32(c.length);
        buf.insert(buf.end(),
            reinterpret_cast<const uint8_t*>(&off_be),
            reinterpret_cast<const uint8_t*>(&off_be) + 8);
        buf.insert(buf.end(),
            reinterpret_cast<const uint8_t*>(&len_be),
            reinterpret_cast<const uint8_t*>(&len_be) + 4);
        buf.insert(buf.end(), c.sha256, c.sha256 + 32);
    }

    return sha1(buf.data(), buf.size());
}

} // namespace thinbt
