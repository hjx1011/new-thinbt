#ifndef THINBT_TSEED_HPP
#define THINBT_TSEED_HPP

#include "common/hash.hpp"
#include "common/platform.hpp"

#include <cstdint>
#include <string>
#include <vector>

#pragma pack(push, 1)

struct TSeedHeader {
    static constexpr uint32_t MAGIC = 0x54425344;

    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t chunk_count;
    uint64_t file_size;
    uint32_t min_chunk_size;
    uint32_t avg_chunk_size;
    uint32_t max_chunk_size;
    uint32_t file_name_len;
    uint32_t announce_len;
    uint8_t  reserved[20];
    uint8_t  file_sha256[32];

    void to_host_endian()
    {
        magic          = thinbt::ntoh32(magic);
        version        = thinbt::ntoh16(version);
        flags          = thinbt::ntoh16(flags);
        chunk_count    = thinbt::ntoh32(chunk_count);
        file_size      = thinbt::ntoh64(file_size);
        min_chunk_size = thinbt::ntoh32(min_chunk_size);
        avg_chunk_size = thinbt::ntoh32(avg_chunk_size);
        max_chunk_size = thinbt::ntoh32(max_chunk_size);
        file_name_len  = thinbt::ntoh32(file_name_len);
        announce_len   = thinbt::ntoh32(announce_len);
    }

    void to_network_endian()
    {
        magic          = thinbt::hton32(magic);
        version        = thinbt::hton16(version);
        flags          = thinbt::hton16(flags);
        chunk_count    = thinbt::hton32(chunk_count);
        file_size      = thinbt::hton64(file_size);
        min_chunk_size = thinbt::hton32(min_chunk_size);
        avg_chunk_size = thinbt::hton32(avg_chunk_size);
        max_chunk_size = thinbt::hton32(max_chunk_size);
        file_name_len  = thinbt::hton32(file_name_len);
        announce_len   = thinbt::hton32(announce_len);
    }
};

static_assert(sizeof(TSeedHeader) == 92, "TSeedHeader must be 92 bytes");

struct ChunkEntry {
    uint64_t offset;
    uint32_t length;
    uint8_t  sha256[32];

    void to_host_endian()
    {
        offset = thinbt::ntoh64(offset);
        length = thinbt::ntoh32(length);
    }

    void to_network_endian()
    {
        offset = thinbt::hton64(offset);
        length = thinbt::hton32(length);
    }
};

static_assert(sizeof(ChunkEntry) == 44, "ChunkEntry must be 44 bytes");

#pragma pack(pop)

struct TSeedFile {
    TSeedHeader           header;
    std::string           file_name;
    std::string           announce_url;
    std::vector<ChunkEntry> chunks;
    thinbt::Sha1Digest   info_hash;

    thinbt::Sha1Digest compute_info_hash() const;
};

#endif
