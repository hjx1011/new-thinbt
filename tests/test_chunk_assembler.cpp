#include "daemon/chunk_assembler.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <random>
#include <algorithm>
#include <cstring>

int main() {
    const uint32_t chunk_size = 128 * 1024;
    const uint32_t num_slots  = chunk_size / thinbt::SUB_BLOCK_SIZE; // 8

    std::vector<uint8_t> buffer(chunk_size, 0);

    // Test 1: Sequential delivery
    {
        std::fill(buffer.begin(), buffer.end(), 0);
        thinbt::ChunkAssembler a;
        a.init(buffer.data(), chunk_size);
        std::vector<uint8_t> data(thinbt::SUB_BLOCK_SIZE);
        for (uint32_t i = 0; i < num_slots; i++) {
            memset(data.data(), static_cast<uint8_t>(i), data.size());
            bool done = a.on_piece(i * thinbt::SUB_BLOCK_SIZE, data.data(),
                                    static_cast<uint32_t>(data.size()));
            assert(done == (i == num_slots - 1));
        }
        assert(a.is_complete());
        for (uint32_t i = 0; i < num_slots; i++)
            assert(buffer[i * thinbt::SUB_BLOCK_SIZE] == static_cast<uint8_t>(i));
        std::cout << "[PASS] Test 1: sequential delivery" << std::endl;
    }

    // Test 2: Random-order delivery
    {
        std::fill(buffer.begin(), buffer.end(), 0);
        thinbt::ChunkAssembler a2;
        a2.init(buffer.data(), chunk_size);
        std::vector<uint32_t> order = {0,1,2,3,4,5,6,7};
        std::shuffle(order.begin(), order.end(), std::mt19937{42});
        std::vector<uint8_t> data(thinbt::SUB_BLOCK_SIZE);
        for (uint32_t i = 0; i < num_slots; i++) {
            uint32_t slot = order[i];
            memset(data.data(), static_cast<uint8_t>(slot), data.size());
            bool done = a2.on_piece(slot * thinbt::SUB_BLOCK_SIZE, data.data(),
                                     static_cast<uint32_t>(data.size()));
            assert(done == (i == num_slots - 1));
        }
        assert(a2.is_complete());
        std::cout << "[PASS] Test 2: random order" << std::endl;
    }

    // Test 3: Duplicate delivery (stale peer after timeout)
    {
        std::fill(buffer.begin(), buffer.end(), 0);
        thinbt::ChunkAssembler a3;
        a3.init(buffer.data(), chunk_size);
        std::vector<uint8_t> data(thinbt::SUB_BLOCK_SIZE);
        for (uint32_t i = 0; i < num_slots; i++) {
            memset(data.data(), static_cast<uint8_t>(i), data.size());
            a3.on_piece(i * thinbt::SUB_BLOCK_SIZE, data.data(),
                        static_cast<uint32_t>(data.size()));
        }
        assert(a3.is_complete());
        memset(data.data(), 0xFF, data.size());
        bool done = a3.on_piece(3 * thinbt::SUB_BLOCK_SIZE, data.data(),
                                 static_cast<uint32_t>(data.size()));
        assert(!done); // should not trigger completion again
        assert(a3.is_complete()); // still marked complete
        std::cout << "[PASS] Test 3: duplicate ignored after completion" << std::endl;
    }

    // Test 4: Concurrent 2-thread delivery
    {
        std::fill(buffer.begin(), buffer.end(), 0);
        thinbt::ChunkAssembler a4;
        a4.init(buffer.data(), chunk_size);
        std::atomic<uint32_t> completions{0};

        auto worker = [&](const std::vector<uint32_t>& slots) {
            std::vector<uint8_t> data(thinbt::SUB_BLOCK_SIZE);
            for (uint32_t slot : slots) {
                memset(data.data(), static_cast<uint8_t>(slot), data.size());
                if (a4.on_piece(slot * thinbt::SUB_BLOCK_SIZE, data.data(),
                                static_cast<uint32_t>(data.size())))
                    completions++;
            }
        };
        std::thread t1(worker, std::vector<uint32_t>{0,1,2,3});
        std::thread t2(worker, std::vector<uint32_t>{4,5,6,7});
        t1.join(); t2.join();
        assert(completions.load() == 1);
        assert(a4.is_complete());
        std::cout << "[PASS] Test 4: concurrent 2-thread" << std::endl;
    }

    std::cout << "All ChunkAssembler tests passed!" << std::endl;
    return 0;
}
