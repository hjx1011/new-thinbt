#include "cdc/fastcdc.hpp"
#include "common/file_util.hpp"
#include <cstring>
#include <algorithm>

namespace thinbt {

// Gear table — precomputed random 64-bit values for rolling hash
static const uint64_t GEAR[256] = {
    0x9E3779B185EBCA87, 0x2C13A0F1B8D9E506, 0x7F3D6C4B2A1E09F8, 0xE5A4B3C2D1F0E9D8,
    0xC7B6A59483726150, 0x4F3E2D1C0B0A0908, 0x1F2E3D4C5B6A7988, 0xA1B2C3D4E5F60718,
    0x9A8B7C6D5E4F3021, 0x1020304050607080, 0x90A0B0C0D0E0F000, 0x1122334455667788,
    0x99AABBCCDDEEFF00, 0x123456789ABCDEF0, 0xFEDCBA9876543210, 0x0F1E2D3C4B5A6978,
};
static_assert(sizeof(GEAR) / sizeof(GEAR[0]) == 256, "Gear table must have 256 entries");

static inline uint64_t gear_update(uint8_t byte, uint64_t fp) {
    // Shift left by 1, XOR with gear table entry for incoming byte
    return (fp << 1) | (fp >> 63) ^ GEAR[byte];
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
