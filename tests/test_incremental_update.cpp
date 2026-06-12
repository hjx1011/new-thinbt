#include "daemon/task_manager.hpp"
#include "daemon/peer_manager.hpp"
#include "seed/tseed.hpp"

#include <asio.hpp>

#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace thinbt;

namespace thinbt {
void write_tseed(const std::string& output_path,
                 const std::string& file_path,
                 const std::string& file_name,
                 const std::string& announce_url,
                 const std::vector<ChunkEntry>& chunks,
                 uint32_t min_chunk_size,
                 uint32_t avg_chunk_size,
                 uint32_t max_chunk_size);
}

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

static std::vector<uint8_t> read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                std::istreambuf_iterator<char>());
}

static void write_file(const std::string& path, const std::vector<uint8_t>& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
}

static ChunkEntry make_chunk(uint64_t offset, const uint8_t* data, uint32_t len) {
    ChunkEntry c{};
    c.offset = offset;
    c.length = len;
    auto digest = sha256(data, len);
    std::memcpy(c.sha256, digest.data(), 32);
    return c;
}

int main() {
    const std::string old_file = "/tmp/thinbt_old.bin";
    const std::string new_file = "/tmp/thinbt_new.bin";
    const std::string old_seed = "/tmp/thinbt_old.tseed";
    const std::string new_seed = "/tmp/thinbt_new.tseed";

    constexpr uint32_t chunk_size = 4096;
    constexpr uint32_t total_size = chunk_size * 2;

    std::vector<uint8_t> old_data(total_size);
    std::vector<uint8_t> new_data(total_size);
    for (uint32_t i = 0; i < chunk_size; ++i) {
        old_data[i] = static_cast<uint8_t>(i & 0xFF);
        old_data[chunk_size + i] = static_cast<uint8_t>((i * 3) & 0xFF);
        new_data[i] = old_data[i];
        new_data[chunk_size + i] = static_cast<uint8_t>((255 - i) & 0xFF);
    }

    write_file(old_file, old_data);
    write_file(new_file, new_data);

    std::vector<ChunkEntry> old_chunks{
        make_chunk(0, old_data.data(), chunk_size),
        make_chunk(chunk_size, old_data.data() + chunk_size, chunk_size),
    };
    std::vector<ChunkEntry> new_chunks{
        make_chunk(0, new_data.data(), chunk_size),
        make_chunk(chunk_size, new_data.data() + chunk_size, chunk_size),
    };

    write_tseed(old_seed, old_file, "old.bin", "thinbt://127.0.0.1:8080/announce",
                old_chunks, chunk_size, chunk_size, chunk_size);
    write_tseed(new_seed, new_file, "new.bin", "thinbt://127.0.0.1:8080/announce",
                new_chunks, chunk_size, chunk_size, chunk_size);

    const std::vector<uint8_t> before = read_file(old_file);

    asio::io_context io;
    {
        TaskManager mgr(io, 0, "", 8080);
        std::string resp = mgr.cmd_update(new_seed, "/tmp/thinbt_update_output.bin", old_seed, old_file);
        if (resp.find("\"status\":\"ok\"") == std::string::npos) {
            return fail("cmd_update should succeed for incremental update test");
        }
    }

    const std::vector<uint8_t> after = read_file(old_file);
    if (before != after) {
        return fail("incremental update must not modify the old source file");
    }

    std::remove(old_file.c_str());
    std::remove(new_file.c_str());
    std::remove(old_seed.c_str());
    std::remove(new_seed.c_str());
    std::remove("/tmp/thinbt_update_output.bin");

    std::cout << "Incremental update tests passed!" << std::endl;
    return 0;
}
