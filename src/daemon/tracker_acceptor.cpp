#include "tracker_acceptor.hpp"
#include "tracker_server.hpp"
#include <sstream>
#include <cstring>
#include <cctype>
#include <iostream>

namespace thinbt {
namespace {

std::string json_extract_string(const std::string& obj, const std::string& key) {
    std::string search = '"' + key + '"';
    auto kp = obj.find(search);
    if (kp == std::string::npos) return "";

    auto colon = obj.find(':', kp + search.size());
    if (colon == std::string::npos) return "";

    auto vs = obj.find('"', colon + 1);
    if (vs == std::string::npos) return "";

    auto ve = obj.find('"', vs + 1);
    if (ve == std::string::npos) return "";

    return obj.substr(vs + 1, ve - vs - 1);
}

uint64_t json_extract_int(const std::string& obj, const std::string& key) {
    std::string search = '"' + key + '"';
    auto kp = obj.find(search);
    if (kp == std::string::npos) return 0;

    auto colon = obj.find(':', kp + search.size());
    if (colon == std::string::npos) return 0;

    size_t ns = colon + 1;
    while (ns < obj.size() && (obj[ns] == ' ' || obj[ns] == '\t')) ns++;

    size_t ne = ns;
    while (ne < obj.size() && (std::isdigit(obj[ne]) || obj[ne] == '-')) ne++;

    if (ne == ns) return 0;
    return std::stoull(obj.substr(ns, ne - ns));
}

} // anonymous namespace

TrackerAcceptor::TrackerAcceptor(asio::io_context& io, TrackerServer& server, uint16_t port)
    : io_(io), server_(server), acceptor_(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {}

void TrackerAcceptor::start() {
    do_accept();
    std::cout << "[tracker] listening on 0.0.0.0:" << acceptor_.local_endpoint().port() << std::endl;
}

void TrackerAcceptor::do_accept() {
    auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
    acceptor_.async_accept(*sock, [this, sock](asio::error_code ec) {
        if (!ec) {
            handle_client(std::move(sock));
        }
        do_accept();
    });
}

void TrackerAcceptor::handle_client(std::shared_ptr<asio::ip::tcp::socket> socket) {
    auto buf = std::make_shared<std::string>();
    asio::async_read_until(*socket, asio::dynamic_buffer(*buf), '\n',
        [this, socket, buf](asio::error_code ec, size_t len) {
            if (ec) return;

            std::string request(buf->data(), len);
            std::string info_hash = json_extract_string(request, "info_hash");
            uint16_t peer_port   = static_cast<uint16_t>(json_extract_int(request, "port"));
            uint32_t speed_mbps  = static_cast<uint32_t>(json_extract_int(request, "speed_mbps"));

            if (info_hash.empty()) {
                std::string err = "{\"error\":\"missing info_hash\"}\n";
                asio::async_write(*socket, asio::buffer(err),
                    [socket](asio::error_code, size_t) {});
                return;
            }

            std::string peer_ip;
            asio::error_code ep_ec;
            auto ep = socket->remote_endpoint(ep_ec);
            if (!ep_ec) {
                peer_ip = ep.address().to_string();
            }

            auto peers = server_.announce(info_hash, peer_ip, peer_port, speed_mbps);

            // 构造响应：排除请求方（server_.announce 已做过滤，这里防御性再过滤一次）
            std::ostringstream resp;
            resp << "{\"op\":\"announce_ok\",\"peers\":[";
            bool first = true;
            for (auto& p : peers) {
                if (p.ip == peer_ip && p.port == peer_port) continue;
                if (!first) resp << ",";
                first = false;
                resp << "{\"ip\":\"" << p.ip << "\",\"port\":" << p.port
                     << ",\"flags\":" << static_cast<int>(p.flags) << "}";
            }
            resp << "]}\n";

            auto resp_str = std::make_shared<std::string>(resp.str());
            asio::async_write(*socket, asio::buffer(*resp_str),
                [socket, resp_str](asio::error_code, size_t) {});
        });
}

} // namespace thinbt
