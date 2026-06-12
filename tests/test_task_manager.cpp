#include "daemon/task_manager.hpp"
#include "daemon/peer_manager.hpp"
#include "common/hash.hpp"
#include "seed/seed_writer.hpp"

#include <asio.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

using namespace thinbt;

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

int main() {
    Scheduler sched;
    sched.init(
        3,
        1000,
        [](uint32_t, uint32_t, uint32_t, uint32_t) {},
        [](uint32_t) {},
        []() {});
    sched.set_chunk_sizes({SUB_BLOCK_SIZE, SUB_BLOCK_SIZE, SUB_BLOCK_SIZE});

    mark_seed_complete(sched, {true, true, true});

    if (sched.missing_count() != 0) {
        return fail("seed scheduler must start with no missing chunks");
    }
    if (sched.phase() != SchedulerPhase::DONE) {
        return fail("seed scheduler must start in DONE phase");
    }

    asio::io_context io;
    TaskManager mgr(io, 16889, "192.168.177.56", 8080);
    auto task_id = TaskManager::gen_task_id();
    if (task_id.size() != 8) {
        return fail("task ids should remain short and stable for log/debug use");
    }

    {
        namespace fs = std::filesystem;
        auto base = fs::temp_directory_path() / ("thinbt-task-manager-" + TaskManager::gen_task_id());
        fs::create_directories(base);
        auto source_path = base / "source.bin";
        auto seed_path = base / "source.tseed";
        auto out_path = base / "download.bin";

        {
            std::ofstream out(source_path, std::ios::binary);
            std::string body(4096, 'x');
            out.write(body.data(), static_cast<std::streamsize>(body.size()));
        }

        ChunkEntry chunk{};
        chunk.offset = 0;
        chunk.length = 4096;
        auto digest = sha256(reinterpret_cast<const uint8_t*>("xxxx"), 4);
        std::copy(digest.begin(), digest.end(), chunk.sha256);

        asio::io_context announce_io;
        asio::ip::tcp::acceptor acceptor(
            announce_io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 0));
        const auto announce_port = acceptor.local_endpoint().port();
        int announce_count = 0;

        std::function<void()> accept_next = [&]() {
            auto socket = std::make_shared<asio::ip::tcp::socket>(announce_io);
            acceptor.async_accept(*socket, [&, socket](const asio::error_code& ec) {
                if (ec) return;
                accept_next();

                auto request = std::make_shared<std::string>();
                asio::async_read_until(*socket, asio::dynamic_buffer(*request), '\n',
                    [&, socket, request](const asio::error_code& read_ec, std::size_t) {
                        if (read_ec) return;
                        ++announce_count;

                        auto response = std::make_shared<std::string>(
                            "{\"interval\":30,\"peers\":[{\"ip\":\"127.0.0.1\",\"port\":1,\"flags\":0}]}\n");
                        asio::async_write(*socket, asio::buffer(*response),
                            [socket, response](const asio::error_code&, std::size_t) {});
                    });
            });
        };
        accept_next();

        write_tseed(seed_path.string(), source_path.string(), "source.bin",
                    "thinbt://127.0.0.1:" + std::to_string(announce_port) + "/announce",
                    {chunk}, 4096, 4096, 4096);

        TaskManager announce_mgr(announce_io, 16890);
        auto add_resp = announce_mgr.cmd_add(seed_path.string(), out_path.string());
        if (add_resp.find("\"status\":\"ok\"") == std::string::npos) {
            return fail("cmd_add should create a small download task for announce tests");
        }

        asio::steady_timer tick_timer(announce_io);
        std::function<void()> tick_loop = [&]() {
            announce_mgr.tick();
            tick_timer.expires_after(std::chrono::milliseconds(100));
            tick_timer.async_wait([&](const asio::error_code& ec) {
                if (!ec) tick_loop();
            });
        };
        tick_loop();

        asio::steady_timer stop_timer(announce_io, std::chrono::milliseconds(2400));
        stop_timer.async_wait([&](const asio::error_code&) {
            announce_io.stop();
        });

        announce_io.run();

        if (announce_count < 2) {
            return fail("download tasks should do short-cycle startup announces before the 30 second heartbeat");
        }

        fs::remove_all(base);
    }

    std::cout << "Task manager tests passed!" << std::endl;
    return 0;
}
