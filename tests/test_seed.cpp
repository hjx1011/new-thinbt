#include "seed/tseed.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <cstring>
#include <memory>

// Forward declarations (defined in seed_reader.cpp / seed_writer.cpp)
namespace thinbt {
std::unique_ptr<TSeedFile> read_tseed(const std::string& path);
void write_tseed(const std::string& output_path,
                 const std::string& file_path,
                 const std::string& file_name,
                 const std::string& announce_url,
                 const std::vector<ChunkEntry>& chunks,
                 uint32_t min_chunk_size,
                 uint32_t avg_chunk_size,
                 uint32_t max_chunk_size);
} // namespace thinbt

int main() {
    const std::string test_file   = "/tmp/thinbt_test_seed_source.bin";
    const std::string tseed_path  = "/tmp/thinbt_test.tseed";

    // ── Create a 512KB deterministic test file ──
    const size_t file_size = 512 * 1024;
    {
        std::ofstream f(test_file, std::ios::binary);
        for (size_t i = 0; i < file_size; i++) {
            f.put(static_cast<char>((i * 7 + 13) % 256));
        }
    }

    // ── Create 4 chunks of 128KB each ──
    std::vector<thinbt::ChunkEntry> chunks;
    for (int i = 0; i < 4; i++) {
        thinbt::ChunkEntry ce{};
        ce.offset = static_cast<uint64_t>(i) * 131072;
        ce.length = 131072;
        // Hash not set here — write_tseed computes file_sha256, and chunks get
        // their hashes from the CDC scanner (not yet integrated)
        memset(ce.sha256, static_cast<uint8_t>(i), 32);
        chunks.push_back(ce);
    }

    // ── Write seed ──
    thinbt::write_tseed(tseed_path, test_file, "test_vm.qcow2",
                        "thinbt://192.168.1.10:8080/announce",
                        chunks, 16384, 131072, 1048576);
    std::cout << "[PASS] Seed written: " << tseed_path << std::endl;

    // ── Read back ──
    auto seed = thinbt::read_tseed(tseed_path);

    assert(seed->header.magic == thinbt::TSeedHeader::MAGIC);
    assert(seed->header.version == 1);
    assert(seed->header.chunk_count == 4);
    assert(seed->header.file_size == file_size);
    assert(seed->header.min_chunk_size == 16384);
    assert(seed->header.avg_chunk_size == 131072);
    assert(seed->header.max_chunk_size == 1048576);
    assert(seed->file_name == "test_vm.qcow2");
    assert(seed->announce_url == "thinbt://192.168.1.10:8080/announce");
    assert(seed->chunks.size() == 4);

    // Verify chunk data survived round-trip
    for (int i = 0; i < 4; i++) {
        assert(seed->chunks[i].offset == static_cast<uint64_t>(i) * 131072);
        assert(seed->chunks[i].length == 131072);
        assert(memcmp(seed->chunks[i].sha256, chunks[i].sha256, 32) == 0);
    }

    // Verify InfoHash is non-zero (computed from file_sha256 + chunks)
    bool all_zero = true;
    for (auto b : seed->info_hash) {
        if (b != 0) { all_zero = false; break; }
    }
    assert(!all_zero);
    std::cout << "[PASS] InfoHash computed: "
              << thinbt::sha1_hex(seed->info_hash) << std::endl;

    // ── InfoHash determinism: same content → same InfoHash ──
    auto seed2 = thinbt::read_tseed(tseed_path);
    assert(memcmp(seed->info_hash.data(), seed2->info_hash.data(), 20) == 0);
    std::cout << "[PASS] InfoHash deterministic" << std::endl;

    // ── Cleanup ──
    std::remove(test_file.c_str());
    std::remove(tseed_path.c_str());

    std::cout << "All seed tests passed!" << std::endl;
    return 0;
}
