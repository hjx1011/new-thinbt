#include "cli/cli_commands.hpp"
#include "cdc/fastcdc.hpp"
#include "seed/seed_writer.hpp"
#include "seed/seed_reader.hpp"
#include "common/hash.hpp"
#include "common/net_util.hpp"

#include <sstream>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace thinbt {

static std::string send_ipc(const std::string& json) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return R"({"status":"error","error":"socket failed"})";

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(16888);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return R"({"status":"error","error":"connect failed"})";
    }

    std::string req = json + "\n";
    send(fd, req.c_str(), req.size(), 0);

    char buf[65536] = {};
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    close(fd);

    return std::string(buf, n > 0 ? n : 0);
}

std::string cli_make(int argc, char* argv[]) {
    if (argc < 3) {
        return R"({"status":"error","error":"usage: tbt make <file> [-o output] [--tracker-port 8080] [--announce-ip auto]"})";
    }

    std::string file_path = argv[2];
    std::string output_path;
    std::string announce_ip = "auto";
    uint16_t tracker_port = 8080;

    for (int i = 3; i < argc; i++) {
        if (std::strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            output_path = argv[++i];
        } else if (std::strcmp(argv[i], "--tracker-port") == 0 && i + 1 < argc) {
            tracker_port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--announce-ip") == 0 && i + 1 < argc) {
            announce_ip = argv[++i];
        }
    }

    if (output_path.empty()) {
        output_path = file_path + ".tseed";
    }

    std::string local_ip = (announce_ip == "auto") ? get_local_ip() : announce_ip;

    if (local_ip.empty()) {
        return R"({"status":"error","error":"cannot detect local IP"})";
    }

    std::string announce_url = "thinbt://" + local_ip + ":" + std::to_string(tracker_port) + "/announce";

    // Extract basename from file_path
    std::string file_name = file_path;
    auto slash = file_path.rfind('/');
    auto bslash = file_path.rfind('\\');
    auto sep = std::string::npos;
    if (slash != std::string::npos) sep = slash;
    if (bslash != std::string::npos && (sep == std::string::npos || bslash > sep)) sep = bslash;
    if (sep != std::string::npos) {
        file_name = file_path.substr(sep + 1);
    }

    FastCDCConfig cdc_config{};
    std::vector<ChunkEntry> chunks;
    try {
        chunks = fastcdc_scan_file(file_path, cdc_config);
    } catch (const std::exception& e) {
        return std::string(R"({"status":"error","error":")") + e.what() + R"("})";
    }

    if (chunks.empty()) {
        return R"({"status":"error","error":"file is empty or unreadable"})";
    }

    // Compute total file size from last chunk
    uint64_t file_size = 0;
    for (const auto& c : chunks) {
        uint64_t end = c.offset + c.length;
        if (end > file_size) file_size = end;
    }

    uint32_t chunk_count = static_cast<uint32_t>(chunks.size());

    try {
        write_tseed(output_path, file_path, file_name, announce_url,
                    chunks, cdc_config.min_size, cdc_config.avg_size, cdc_config.max_size);
    } catch (const std::exception& e) {
        return std::string(R"({"status":"error","error":")") + e.what() + R"("})";
    }

    // Compute info_hash: SHA-1(file_sha256 || ChunkEntry[] in network byte order)
    auto file_digest = sha256_file(file_path);
    std::vector<uint8_t> hash_buf;
    hash_buf.reserve(32 + chunks.size() * 44);
    hash_buf.insert(hash_buf.end(), file_digest.data(), file_digest.data() + 32);
    for (const auto& c : chunks) {
        uint64_t off_be = hton64(c.offset);
        uint32_t len_be = hton32(c.length);
        hash_buf.insert(hash_buf.end(),
            reinterpret_cast<const uint8_t*>(&off_be),
            reinterpret_cast<const uint8_t*>(&off_be) + 8);
        hash_buf.insert(hash_buf.end(),
            reinterpret_cast<const uint8_t*>(&len_be),
            reinterpret_cast<const uint8_t*>(&len_be) + 4);
        hash_buf.insert(hash_buf.end(), c.sha256, c.sha256 + 32);
    }
    auto info_hash = sha1_hex(sha1(hash_buf.data(), hash_buf.size()));

    std::ostringstream oss;
    oss << R"({"status":"ok","data":{)";
    oss << R"("seed_path":")" << output_path << R"(")";
    oss << R"(,"info_hash":")" << info_hash << R"(")";
    oss << R"(,"chunk_count":)" << chunk_count;
    oss << R"(,"file_size":)" << file_size;
    oss << R"(}})";
    return oss.str();
}

std::string cli_info(int argc, char* argv[]) {
    if (argc < 3) {
        return R"({"status":"error","error":"usage: tbt info <.tseed>"})";
    }

    std::string seed_path = argv[2];
    std::unique_ptr<TSeedFile> seed;
    try {
        seed = read_tseed(seed_path);
    } catch (const std::exception& e) {
        return std::string(R"({"status":"error","error":")") + e.what() + R"("})";
    }

    auto info_hash = sha1_hex(seed->info_hash);

    std::ostringstream oss;
    oss << R"({"status":"ok","data":{)";
    oss << R"("seed_path":")" << seed_path << R"(")";
    oss << R"(,"file_name":")" << seed->file_name << R"(")";
    oss << R"(,"file_size":)" << seed->header.file_size;
    oss << R"(,"info_hash":")" << info_hash << R"(")";
    oss << R"(,"chunk_count":)" << seed->header.chunk_count;
    oss << R"(,"min_chunk_size":)" << seed->header.min_chunk_size;
    oss << R"(,"avg_chunk_size":)" << seed->header.avg_chunk_size;
    oss << R"(,"max_chunk_size":)" << seed->header.max_chunk_size;
    oss << R"(,"announce_url":")" << seed->announce_url << R"(")";
    oss << R"(}})";
    return oss.str();
}

std::string cli_update(int argc, char* argv[]) {
    if (argc < 6) {
        return R"({"status":"error","error":"usage: tbt update <new.tseed> <new_file> <old.tseed> <old_file>"})";
    }

    std::string new_seed  = argv[2];
    std::string new_file  = argv[3];
    std::string old_seed  = argv[4];
    std::string old_file  = argv[5];

    std::string req = R"({"cmd":"update","args":{)";
    req += R"("new_seed":")" + new_seed + R"(")";
    req += R"(,"new_file":")" + new_file + R"(")";
    req += R"(,"old_seed":")" + old_seed + R"(")";
    req += R"(,"old_file":")" + old_file + R"(")";
    req += "}}";

    return send_ipc(req);
}

} // namespace thinbt
