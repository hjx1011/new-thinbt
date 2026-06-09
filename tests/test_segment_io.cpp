#include "daemon/segment_io.hpp"
#include <cstdio>
#include <cstring>
#include <cassert>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

using namespace thinbt;

static constexpr uint64_t FILE_SIZE   = 2 * 1024 * 1024;  // 2MB
static constexpr uint64_t SEG_SIZE    = 1 * 1024 * 1024;  // 1MB per segment
static constexpr uint32_t CHUNK_SIZE  = 128 * 1024;       // 128KB typical chunk

static const char* TEST_FILE = "/tmp/thinbt_test_segment_io.bin";

static void fill_pattern(uint8_t* buf, uint32_t size, uint64_t seed) {
    for (uint32_t i = 0; i < size; i++)
        buf[i] = static_cast<uint8_t>((seed + i) & 0xFF);
}

static bool verify_pattern(const uint8_t* buf, uint32_t size, uint64_t seed) {
    for (uint32_t i = 0; i < size; i++) {
        uint8_t expected = static_cast<uint8_t>((seed + i) & 0xFF);
        if (buf[i] != expected) {
            std::cerr << "  mismatch at offset " << i
                      << ": got " << static_cast<int>(buf[i])
                      << ", expected " << static_cast<int>(expected) << std::endl;
            return false;
        }
    }
    return true;
}

int main() {
    std::cout << "=== test_segment_io ===" << std::endl;

    // ── Test 1: Basic open / close ──
    {
        std::cout << "Test 1: open..." << std::endl;
        SegmentWriter sw;
        if (!sw.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open" << std::endl; return 1; }
        if (sw.file_size() != FILE_SIZE)               { std::cerr << "FAIL file_size" << std::endl; return 1; }
        if (sw.segment_count() != 2)                   { std::cerr << "FAIL segment_count" << std::endl; return 1; }
        if (sw.get_file_fd() < 0)                      { std::cerr << "FAIL fd" << std::endl; return 1; }
        sw.close();
        std::cout << "  PASS" << std::endl;
    }

    // ── Test 2: Write to both segments through mmap ──
    {
        std::cout << "Test 2: write to segments..." << std::endl;
        SegmentWriter sw;
        if (!sw.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open" << std::endl; return 1; }

        uint8_t* base0 = sw.get_chunk_base(0, CHUNK_SIZE);
        if (!base0) { std::cerr << "FAIL base0 null" << std::endl; return 1; }
        fill_pattern(base0, CHUNK_SIZE, 0);

        uint8_t* base1 = sw.get_chunk_base(SEG_SIZE, CHUNK_SIZE);
        if (!base1) { std::cerr << "FAIL base1 null" << std::endl; return 1; }
        fill_pattern(base1, CHUNK_SIZE, SEG_SIZE);

        // Switch back to segment 0 — msync before munmap preserves data
        uint8_t* base0_again = sw.get_chunk_base(0, CHUNK_SIZE);
        if (!base0_again) { std::cerr << "FAIL base0_again null" << std::endl; return 1; }
        if (!verify_pattern(base0_again, CHUNK_SIZE, 0)) { std::cerr << "FAIL verify" << std::endl; return 1; }

        sw.close();
        std::cout << "  PASS" << std::endl;
    }

    // ── Test 3: Data persistence after close/reopen ──
    {
        std::cout << "Test 3: persistence after reopen..." << std::endl;

        SegmentWriter sw;
        if (!sw.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open" << std::endl; return 1; }

        uint8_t* base0 = sw.get_chunk_base(0, CHUNK_SIZE);
        if (!base0) { std::cerr << "FAIL base0 null" << std::endl; return 1; }
        if (!verify_pattern(base0, CHUNK_SIZE, 0)) { std::cerr << "FAIL verify0" << std::endl; return 1; }

        uint8_t* base1 = sw.get_chunk_base(SEG_SIZE, CHUNK_SIZE);
        if (!base1) { std::cerr << "FAIL base1 null" << std::endl; return 1; }
        if (!verify_pattern(base1, CHUNK_SIZE, SEG_SIZE)) { std::cerr << "FAIL verify1" << std::endl; return 1; }

        sw.close();
        std::cout << "  PASS" << std::endl;
    }

    // ── Test 4: Chunk spanning segment boundary (over-map) ──
    {
        std::cout << "Test 4: cross-segment chunk..." << std::endl;
        SegmentWriter sw;
        if (!sw.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open" << std::endl; return 1; }

        uint64_t cross_offset = SEG_SIZE - 64 * 1024;
        uint32_t cross_size   = 128 * 1024;
        uint8_t* base = sw.get_chunk_base(cross_offset, cross_size);
        if (!base) { std::cerr << "FAIL base null" << std::endl; return 1; }

        fill_pattern(base, cross_size, cross_offset);

        sw.close();

        SegmentWriter sw2;
        if (!sw2.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open2" << std::endl; return 1; }
        uint8_t* base2 = sw2.get_chunk_base(cross_offset, cross_size);
        if (!base2) { std::cerr << "FAIL base2 null" << std::endl; return 1; }
        if (!verify_pattern(base2, cross_size, cross_offset)) { std::cerr << "FAIL verify" << std::endl; return 1; }

        sw2.close();
        std::cout << "  PASS" << std::endl;
    }

    // ── Test 5: Overwrite idempotency ──
    {
        std::cout << "Test 5: overwrite idempotency..." << std::endl;
        SegmentWriter sw;
        if (!sw.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open" << std::endl; return 1; }

        uint8_t* base = sw.get_chunk_base(256 * 1024, CHUNK_SIZE);
        if (!base) { std::cerr << "FAIL base null" << std::endl; return 1; }

        fill_pattern(base, CHUNK_SIZE, 256 * 1024);
        fill_pattern(base, CHUNK_SIZE, 256 * 1024);  // same data, overwrite

        if (!verify_pattern(base, CHUNK_SIZE, 256 * 1024)) { std::cerr << "FAIL verify" << std::endl; return 1; }
        sw.close();
        std::cout << "  PASS" << std::endl;
    }

    // ── Test 6: Out-of-range rejection ──
    {
        std::cout << "Test 6: out-of-range rejection..." << std::endl;
        SegmentWriter sw;
        if (!sw.open(TEST_FILE, FILE_SIZE, SEG_SIZE)) { std::cerr << "FAIL open" << std::endl; return 1; }

        if (sw.get_chunk_base(FILE_SIZE, CHUNK_SIZE) != nullptr) { std::cerr << "FAIL should be null" << std::endl; return 1; }
        if (sw.get_chunk_base(FILE_SIZE - 1, CHUNK_SIZE) != nullptr) { std::cerr << "FAIL should be null" << std::endl; return 1; }

        sw.close();
        std::cout << "  PASS" << std::endl;
    }

    // Cleanup
    ::unlink(TEST_FILE);

    std::cout << "\nAll tests passed!" << std::endl;
    return 0;
}
