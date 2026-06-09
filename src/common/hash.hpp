#ifndef THINBT_HASH_HPP
#define THINBT_HASH_HPP

#include <array>
#include <cstdint>
#include <string>

namespace thinbt {

using Sha256Digest = std::array<uint8_t, 32>;
using Sha1Digest   = std::array<uint8_t, 20>;

Sha256Digest sha256(const uint8_t* data, size_t len);
Sha256Digest sha256_file(const std::string& path);
Sha1Digest   sha1(const uint8_t* data, size_t len);

std::string sha256_hex(const Sha256Digest& d);
std::string sha1_hex(const Sha1Digest& d);

uint64_t xxhash64(const uint8_t* data, size_t len, uint64_t seed = 0);

} // namespace thinbt

#endif
