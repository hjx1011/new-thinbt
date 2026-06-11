#include "hash.hpp"

#include <openssl/evp.h>
#include <vector>

#include <cstring>
#include <sstream>
#include <stdexcept>
#include <fstream>

namespace thinbt {

// ---------------------------------------------------------------------------
// SHA-256
// ---------------------------------------------------------------------------
Sha256Digest sha256(const uint8_t* data, size_t len)
{
    Sha256Digest d{};
    unsigned int out_len = 0;
    if (!EVP_Digest(data, len, d.data(), &out_len, EVP_sha256(), nullptr))
        throw std::runtime_error("sha256: EVP_Digest failed");
    return d;
}

Sha256Digest sha256_file(const std::string& path)
{
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f)
        throw std::runtime_error("sha256_file: cannot open " + path);

    size_t size = static_cast<size_t>(f.tellg());
    f.seekg(0, std::ios::beg);

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("sha256_file: EVP_MD_CTX_new failed");
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("sha256_file: EVP_DigestInit_ex failed");
    }

    constexpr size_t BUF = 65536;
    std::vector<uint8_t> buf(BUF);
    while (size > 0) {
        size_t chunk = size > BUF ? BUF : size;
        f.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(chunk));
        std::streamsize got = f.gcount();
        if (got <= 0) break;
        if (EVP_DigestUpdate(ctx, buf.data(), static_cast<size_t>(got)) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("sha256_file: EVP_DigestUpdate failed");
        }
        size -= static_cast<size_t>(got);
    }

    Sha256Digest d{};
    unsigned int out_len = 0;
    if (EVP_DigestFinal_ex(ctx, d.data(), &out_len) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("sha256_file: EVP_DigestFinal_ex failed");
    }
    EVP_MD_CTX_free(ctx);
    return d;
}

std::string sha256_hex(const Sha256Digest& d)
{
    std::ostringstream oss;
    oss.fill('0');
    oss << std::hex;
    for (auto b : d) {
        oss.width(2);
        oss << static_cast<unsigned>(b);
    }
    return oss.str();
}

// ---------------------------------------------------------------------------
// SHA-1
// ---------------------------------------------------------------------------
Sha1Digest sha1(const uint8_t* data, size_t len)
{
    Sha1Digest d{};
    unsigned int out_len = 0;
    if (!EVP_Digest(data, len, d.data(), &out_len, EVP_sha1(), nullptr))
        throw std::runtime_error("sha1: EVP_Digest failed");
    return d;
}

std::string sha1_hex(const Sha1Digest& d)
{
    std::ostringstream oss;
    oss.fill('0');
    oss << std::hex;
    for (auto b : d) {
        oss.width(2);
        oss << static_cast<unsigned>(b);
    }
    return oss.str();
}

// ---------------------------------------------------------------------------
// xxHash64 (full algorithm)
// ---------------------------------------------------------------------------
static constexpr uint64_t XXH_PRIME64_1 = 0x9E3779B185EBCA87ULL;
static constexpr uint64_t XXH_PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;
static constexpr uint64_t XXH_PRIME64_3 = 0x165667B19E3779F9ULL;
static constexpr uint64_t XXH_PRIME64_4 = 0x85EBCA77C2B2AE63ULL;
static constexpr uint64_t XXH_PRIME64_5 = 0x27D4EB2F165667C5ULL;

static inline uint64_t xxh_rotl64(uint64_t x, int r)
{
    return (x << r) | (x >> (64 - r));
}

static inline uint64_t xxh64_round(uint64_t acc, uint64_t input)
{
    acc += input * XXH_PRIME64_2;
    acc  = xxh_rotl64(acc, 31);
    acc *= XXH_PRIME64_1;
    return acc;
}

static inline uint64_t xxh64_avalanche(uint64_t h)
{
    h ^= h >> 33;
    h *= XXH_PRIME64_2;
    h ^= h >> 29;
    h *= XXH_PRIME64_3;
    h ^= h >> 32;
    return h;
}

static inline uint64_t xxh_read64le(const uint8_t* p)
{
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

uint64_t xxhash64(const uint8_t* data, size_t len, uint64_t seed)
{
    const uint8_t*       end = data + len;
    const uint8_t* const limit = data + len - 32;
    uint64_t h64;

    if (len >= 32) {
        uint64_t v1 = seed + XXH_PRIME64_1 + XXH_PRIME64_2;
        uint64_t v2 = seed + XXH_PRIME64_2;
        uint64_t v3 = seed;
        uint64_t v4 = seed - XXH_PRIME64_1;

        for (; data <= limit - 32; data += 32) {
            v1 = xxh64_round(v1, xxh_read64le(data));
            v2 = xxh64_round(v2, xxh_read64le(data + 8));
            v3 = xxh64_round(v3, xxh_read64le(data + 16));
            v4 = xxh64_round(v4, xxh_read64le(data + 24));
        }

        h64 = xxh_rotl64(v1, 1) + xxh_rotl64(v2, 7) +
              xxh_rotl64(v3, 12) + xxh_rotl64(v4, 18);

        // merge rounds
        v1 *= XXH_PRIME64_2; v1 = xxh_rotl64(v1, 31); v1 *= XXH_PRIME64_1; h64 ^= v1;
        h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;

        v2 *= XXH_PRIME64_2; v2 = xxh_rotl64(v2, 31); v2 *= XXH_PRIME64_1; h64 ^= v2;
        h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;

        v3 *= XXH_PRIME64_2; v3 = xxh_rotl64(v3, 31); v3 *= XXH_PRIME64_1; h64 ^= v3;
        h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;

        v4 *= XXH_PRIME64_2; v4 = xxh_rotl64(v4, 31); v4 *= XXH_PRIME64_1; h64 ^= v4;
        h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;
    } else {
        h64 = seed + XXH_PRIME64_5;
    }

    h64 += static_cast<uint64_t>(len);

    // 8-byte tail
    for (; data + 8 <= end; data += 8) {
        uint64_t k1 = xxh64_round(0, xxh_read64le(data));
        h64 ^= k1;
        h64  = xxh_rotl64(h64, 27) * XXH_PRIME64_1 + XXH_PRIME64_4;
    }

    // 4-byte tail
    if (data + 4 <= end) {
        uint32_t v;
        std::memcpy(&v, data, sizeof(v));
        h64 ^= static_cast<uint64_t>(v) * XXH_PRIME64_1;
        h64  = xxh_rotl64(h64, 23) * XXH_PRIME64_2 + XXH_PRIME64_3;
        data += 4;
    }

    // 1-byte tail
    for (; data < end; ++data) {
        h64 ^= static_cast<uint64_t>(*data) * XXH_PRIME64_5;
        h64  = xxh_rotl64(h64, 11) * XXH_PRIME64_1;
    }

    return xxh64_avalanche(h64);
}

} // namespace thinbt
