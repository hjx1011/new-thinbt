#include "cdc/fastcdc.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <cstring>

int main() {
    const std::string test_file = "/tmp/thinbt_test_cdc.bin";
    const size_t file_size = 1024 * 1024; // 1 MB

    // Generate deterministic pseudo-random test data
    {
        std::ofstream f(test_file, std::ios::binary);
        for (size_t i = 0; i < file_size; i++) {
            f.put(static_cast<char>((i * 7 + 13) % 256));
        }
    }

    thinbt::FastCDCConfig cfg;
    cfg.min_size = 16 * 1024;
    cfg.avg_size = 128 * 1024;
    cfg.max_size = 1024 * 1024;

    // Run FastCDC
    auto chunks = thinbt::fastcdc_scan_file(test_file, cfg);
    std::cout << "FastCDC produced " << chunks.size() << " chunks" << std::endl;

    // 1. Chunks cover the entire file
    assert(!chunks.empty());
    assert(chunks[0].offset == 0);

    uint64_t total_bytes = 0;
    for (const auto& c : chunks) total_bytes += c.length;
    assert(total_bytes == file_size);
    std::cout << "[PASS] Full coverage: " << total_bytes << " bytes" << std::endl;

    // 2. No gaps between chunks
    for (size_t i = 1; i < chunks.size(); i++) {
        assert(chunks[i].offset == chunks[i-1].offset + chunks[i-1].length);
    }
    std::cout << "[PASS] No gaps between chunks" << std::endl;

    // 3. Chunk sizes within bounds (last chunk may be smaller)
    for (size_t i = 0; i < chunks.size(); i++) {
        if (i < chunks.size() - 1) {
            assert(chunks[i].length >= cfg.min_size);
        }
        assert(chunks[i].length <= cfg.max_size);
    }
    std::cout << "[PASS] Chunk sizes within bounds" << std::endl;

    // 4. All hashes are non-zero
    for (const auto& c : chunks) {
        bool all_zero = true;
        for (int j = 0; j < 32; j++) {
            if (c.sha256[j] != 0) { all_zero = false; break; }
        }
        assert(!all_zero);
    }
    std::cout << "[PASS] All chunk hashes non-zero" << std::endl;

    // 5. Determinism: re-scan produces identical chunks
    auto chunks2 = thinbt::fastcdc_scan_file(test_file, cfg);
    assert(chunks.size() == chunks2.size());
    for (size_t i = 0; i < chunks.size(); i++) {
        assert(chunks[i].offset == chunks2[i].offset);
        assert(chunks[i].length == chunks2[i].length);
        assert(memcmp(chunks[i].sha256, chunks2[i].sha256, 32) == 0);
    }
    std::cout << "[PASS] Deterministic: re-scan identical" << std::endl;

    std::remove(test_file.c_str());
    std::cout << "All FastCDC tests passed!" << std::endl;
    return 0;
}
