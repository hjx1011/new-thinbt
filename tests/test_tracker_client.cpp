#include "daemon/tracker_client.hpp"

#include <asio.hpp>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

using namespace thinbt;

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

int main() {
    {
        asio::io_context io;
        asio::ip::tcp::acceptor acceptor(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 0));
        const auto port = acceptor.local_endpoint().port();

        bool request_seen = false;
        bool callback_called = false;
        std::size_t callback_peer_count = 999;

        std::function<void()> do_accept = [&]() {
            auto socket = std::make_shared<asio::ip::tcp::socket>(io);
            acceptor.async_accept(*socket, [&, socket](const asio::error_code& ec) {
                if (ec) return;

                auto request = std::make_shared<std::string>();
                asio::async_read_until(*socket, asio::dynamic_buffer(*request), '\n',
                    [&, socket, request](const asio::error_code& read_ec, std::size_t) {
                        if (read_ec) return;
                        request_seen = true;

                        auto response = std::make_shared<std::string>("{\"interval\":30,\"peers\":[]}\n");
                        asio::async_write(*socket, asio::buffer(*response),
                            [socket, response](const asio::error_code&, std::size_t) {});
                    });
            });
        };

        do_accept();

        auto client = std::make_shared<TrackerClient>(io, "0123456789abcdef0123456789abcdef01234567", 16889, 1000);
        client->announce("127.0.0.1", port, [&](const std::vector<PexPeer>& peers) {
            callback_called = true;
            callback_peer_count = peers.size();
        });

        asio::steady_timer stop_timer(io, std::chrono::milliseconds(500));
        stop_timer.async_wait([&](const asio::error_code&) {
            io.stop();
        });

        io.run();

        if (!request_seen) {
            return fail("tracker client must send an announce request");
        }
        if (!callback_called) {
            return fail("empty peer lists should complete the announce instead of forcing a retry delay");
        }
        if (callback_peer_count != 0) {
            return fail("empty peer lists should be delivered unchanged to the caller");
        }
    }

    {
        asio::io_context io;
        asio::ip::tcp::acceptor acceptor(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 0));
        const auto port = acceptor.local_endpoint().port();

        int request_count = 0;
        bool saw_peer_callback = false;
        std::size_t final_peer_count = 0;

        std::function<void()> do_accept = [&]() {
            auto socket = std::make_shared<asio::ip::tcp::socket>(io);
            acceptor.async_accept(*socket, [&, socket](const asio::error_code& ec) {
                if (ec) return;
                do_accept();

                auto request = std::make_shared<std::string>();
                asio::async_read_until(*socket, asio::dynamic_buffer(*request), '\n',
                    [&, socket, request](const asio::error_code& read_ec, std::size_t) {
                        if (read_ec) return;
                        request_count++;

                        const char* body = request_count == 1
                            ? "{\"interval\":30,\"peers\":[]}\n"
                            : "{\"interval\":30,\"peers\":[{\"ip\":\"127.0.0.1\",\"port\":16889,\"flags\":0}]}\n";
                        auto response = std::make_shared<std::string>(body);
                        asio::async_write(*socket, asio::buffer(*response),
                            [socket, response](const asio::error_code&, std::size_t) {});
                    });
            });
        };

        do_accept();

        auto client = std::make_shared<TrackerClient>(io, "0123456789abcdef0123456789abcdef01234567", 16889, 1000);
        client->announce("127.0.0.1", port, [&](const std::vector<PexPeer>& peers) {
            if (!peers.empty()) {
                saw_peer_callback = true;
                final_peer_count = peers.size();
                io.stop();
            }
        });

        asio::steady_timer stop_timer(io, std::chrono::milliseconds(2500));
        stop_timer.async_wait([&](const asio::error_code&) {
            io.stop();
        });

        io.run();

        if (request_count < 2) {
            return fail("empty peer responses should trigger a quick tracker poll instead of waiting for the 30 second heartbeat");
        }
        if (!saw_peer_callback || final_peer_count != 1) {
            return fail("tracker client should discover peers promptly after an empty response");
        }
    }

    std::cout << "Tracker client tests passed!" << std::endl;
    return 0;
}
