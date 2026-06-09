#include "cdc/fastcdc.hpp"
#include "common/file_util.hpp"
#include <cstring>
#include <algorithm>

namespace thinbt {

// Gear table — precomputed random 64-bit values for rolling hash
static const std::array<uint64_t, 256> GEAR = []() {
    std::array<uint64_t, 256> arr{};
    uint64_t seed = 0x9E3779B185EBCA87ULL;
    for (int i = 0; i < 256; i++) {
        seed = (seed ^ (seed >> 12)) * 0x2545F4914F6CDD1DULL;
        arr[i] = seed;
    }
    return arr;
}();
static_assert(sizeof(GEAR) / sizeof(GEAR[0]) == 256, "Gear table must have 256 entries");

static inline uint64_t gear_update(uint8_t byte, uint64_t fp) {
    return (fp << 1) | ((fp >> 63) ^ GEAR[byte]);
}

struct ScanState {
    const uint8_t* data;
    uint64_t size;
    uint32_t min_size;
    uint32_t avg_size;
    uint32_t max_size;
    uint32_t mask_bits;
    uint32_t chunk_mask;
};

static void init_scan_state(ScanState& st, const FastCDCConfig& cfg) {
    // avg_size / 2^(n+1) ≈ avg/4  using bitmask
    st.mask_bits = 0;
    uint32_t v = cfg.avg_size;
    while (v > 1) { v >>= 1; st.mask_bits++; }
    if (st.mask_bits > 1) st.mask_bits -= 2;
    st.chunk_mask = (1u << st.mask_bits) - 1;
    st.min_size = cfg.min_size;
    st.avg_size = cfg.avg_size;
    st.max_size = cfg.max_size;
}

std::vector<ChunkEntry> fastcdc_scan_file(const std::string& file_path,
                                           const FastCDCConfig& config) {
    std::vector<ChunkEntry> chunks;

    MappedFile mf;
    if (!mf.open_and_map(file_path, false)) {
        // Fallback: read into buffer if mmap fails
        FILE* f = fopen(file_path.c_str(), "rb");
        if (!f) return chunks;
        fseek(f, 0, SEEK_END);
        uint64_t size = ftell(f);
        fseek(f, 0, SEEK_SET);
        std::vector<uint8_t> buf(size);
        fread(buf.data(), 1, size, f);
        fclose(f);

        fastcdc_scan_buffer(buf.data(), size, config,
            [&](uint64_t off, uint32_t len, const Sha256Digest& h) {
                ChunkEntry ce{};
                ce.offset = off;
                ce.length = len;
                memcpy(ce.sha256, h.data(), 32);
                chunks.push_back(ce);
            });
        return chunks;
    }

    fastcdc_scan_buffer(reinterpret_cast<const uint8_t*>(mf.data()),
                         mf.size(), config,
        [&](uint64_t off, uint32_t len, const Sha256Digest& h) {
            ChunkEntry ce{};
            ce.offset = off;
            ce.length = len;
            memcpy(ce.sha256, h.data(), 32);
            chunks.push_back(ce);
        });

    return chunks;
}

void fastcdc_scan_buffer(const uint8_t* data, uint64_t size,
                          const FastCDCConfig& config,
                          ChunkCallback callback) {
    if (size == 0) return;

    ScanState st{};
    st.data = data;
    st.size = size;
    init_scan_state(st, config);

    uint64_t offset_start = 0;
    uint64_t fingerprint  = 0;
    uint64_t i = 0;

    while (i < size) {
        fingerprint = gear_update(data[i], fingerprint);

        uint64_t pos = i - offset_start + 1;
        bool at_min = pos >= st.min_size;
        bool at_max = pos >= st.max_size;
        bool matched = at_min && ((fingerprint & st.chunk_mask) == 0);

        if (matched || at_max || i == size - 1) {
            uint64_t chunk_len = (i == size - 1) ? (size - offset_start) : pos;

            auto hash = sha256(data + offset_start, static_cast<size_t>(chunk_len));
            callback(offset_start, static_cast<uint32_t>(chunk_len), hash);

            offset_start = i + 1;
            fingerprint  = 0;
        }
        i++;
    }
}

} // namespace thinbt
