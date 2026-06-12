#include "daemon/peer_session.hpp"

#include <asio.hpp>

#include <chrono>
#include <iostream>
#include <thread>
#include <memory>

using namespace thinbt;

static int fail(const char* msg) {
    std::cerr << "[FAIL] " << msg << std::endl;
    return 1;
}

int main() {
    Sha1Digest info_hash{};

    {
        asio::io_context io;
        asio::ip::tcp::acceptor acceptor(
            io, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
        const uint16_t accept_port = acceptor.local_endpoint().port();

        std::shared_ptr<PeerSession> inbound;
        auto outbound = std::make_shared<PeerSession>(io, info_hash, 1000, 17889);
        bool inbound_done = false;
        bool outbound_done = false;

        acceptor.async_accept([&](asio::error_code ec, asio::ip::tcp::socket sock) {
            if (ec) return;
            inbound = std::make_shared<PeerSession>(io, info_hash, 1000, 18889);
            inbound->set_on_handshake_done([&](std::shared_ptr<PeerSession>) {
                inbound_done = true;
            });
            inbound->start_inbound(std::move(sock), [](std::shared_ptr<PeerSession>) {});
        });
        outbound->set_on_handshake_done([&](std::shared_ptr<PeerSession>) {
            outbound_done = true;
        });
        outbound->start_outbound("127.0.0.1", accept_port, [](std::shared_ptr<PeerSession>) {});

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline && (!inbound_done || !outbound_done)) {
            io.run_for(std::chrono::milliseconds(20));
            io.restart();
        }

        if (!inbound_done || !outbound_done) {
            return fail("peer session loopback handshake should complete");
        }
        if (!inbound || inbound->remote_listen_port() != 17889) {
            return fail("inbound session must learn outbound peer's listening port from handshake");
        }
        if (outbound->remote_listen_port() != 18889) {
            return fail("outbound session must learn inbound peer's listening port from handshake");
        }
        if (inbound->remote_port() == inbound->remote_listen_port()) {
            return fail("test must distinguish ephemeral TCP source port from advertised listening port");
        }
    }

    asio::io_context io;
    PeerSession session(io, info_hash, 1000);

    if (!session.is_peer_choking_me()) {
        return fail("new peer sessions should assume the remote peer is choking us until UNCHOKE arrives");
    }
    session.set_choking_peer(false);
    if (session.is_choking_peer()) {
        return fail("set_choking_peer(false) should unchoke the remote peer for uploads");
    }
    if (!session.is_peer_choking_me()) {
        return fail("local upload choke state must not change whether the remote peer is choking our downloads");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    session.add_sent_bytes(32 * 1024);
    session.update_download_rate();
    session.update_upload_rate();

    if (session.upload_rate_kbps() > 100000) {
        return fail("upload rate should not overflow when download/upload stats are updated back-to-back");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    session.add_sent_bytes(32 * 1024);
    session.update_upload_rate();

    if (session.upload_rate_kbps() == 0) {
        return fail("upload rate should report a non-zero value when bytes were sent");
    }

    std::cout << "Peer session tests passed!" << std::endl;
    return 0;
}
