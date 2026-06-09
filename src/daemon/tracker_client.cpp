#include "tracker_client.hpp"
#include <sstream>
#include <cstring>
#include <arpa/inet.h>
#include <iostream>

namespace thinbt {

TrackerClient::TrackerClient(asio::io_context& io, const std::string& info_hash_hex,
                              uint16_t p2p_port, uint32_t speed_mbps)
    : io_(io), info_hash_hex_(info_hash_hex), p2p_port_(p2p_port), speed_mbps_(speed_mbps) {}

void TrackerClient::announce(const std::string& host, uint16_t port, OnPeers on_peers) {
    retry_count_ = 0;
    do_announce(host, port, std::move(on_peers));
}

void TrackerClient::do_announce(std::string host, uint16_t port, OnPeers on_peers) {
    auto self = shared_from_this();
    auto sock = std::make_shared<asio::ip::tcp::socket>(io_);

    asio::ip::tcp::resolver resolver(io_);
    auto endpoints = resolver.resolve(host, std::to_string(port));

    asio::async_connect(*sock, endpoints,
        [self, sock, on_peers = std::move(on_peers), host, port](asio::error_code ec, auto) mutable {
            if (ec) { self->schedule_retry(std::move(on_peers), host, port); return; }

            std::ostringstream req;
            req << "{\"op\":\"announce\",\"info_hash\":\"" << self->info_hash_hex_
                << "\",\"port\":" << self->p2p_port_
                << ",\"speed_mbps\":" << self->speed_mbps_ << "}\n";
            auto req_str = std::make_shared<std::string>(req.str());

            asio::async_write(*sock, asio::buffer(*req_str),
                [self, sock, req_str, on_peers = std::move(on_peers), host, port](asio::error_code ec2, size_t) mutable {
                    if (ec2) { self->schedule_retry(std::move(on_peers), host, port); return; }

                    auto buf = std::make_shared<std::vector<uint8_t>>(4096);
                    sock->async_read_some(asio::buffer(*buf),
                        [self, sock, buf, on_peers = std::move(on_peers), host, port](asio::error_code ec3, size_t len) mutable {
                            if (ec3) { self->schedule_retry(std::move(on_peers), host, port); return; }

                            std::string resp(reinterpret_cast<char*>(buf->data()), len);
                            std::vector<PexPeer> peers;

                            auto arr_pos = resp.find("\"peers\":[");
                            if (arr_pos != std::string::npos) {
                                size_t p = arr_pos + 9;
                                while (p < resp.size() && resp[p] != ']') {
                                    auto ip_pos = resp.find("\"ip\":\"", p);
                                    if (ip_pos == std::string::npos || ip_pos > resp.find(']', p)) break;
                                    auto ip_s = ip_pos + 6;
                                    auto ip_e = resp.find('"', ip_s);
                                    auto port_pos = resp.find("\"port\":", ip_e);
                                    uint16_t peer_port = 0;
                                    if (port_pos != std::string::npos && port_pos < resp.find(']', p))
                                        peer_port = static_cast<uint16_t>(std::stoi(resp.substr(port_pos + 7)));
                                    auto flags_pos = resp.find("\"flags\":", port_pos);
                                    uint8_t flags_v = 0;
                                    if (flags_pos != std::string::npos && flags_pos < resp.find(']', p))
                                        flags_v = static_cast<uint8_t>(std::stoi(resp.substr(flags_pos + 8)));

                                    std::string ip_str = resp.substr(ip_s, ip_e - ip_s);
                                    PexPeer pp{};
                                    pp.ip = inet_addr(ip_str.c_str());
                                    pp.port = peer_port;
                                    pp.flags = flags_v;
                                    peers.push_back(pp);

                                    p = resp.find('}', ip_e);
                                    if (p == std::string::npos) break;
                                    p++;
                                }
                            }

                            if (peers.empty())
                                self->schedule_retry(std::move(on_peers), host, port);
                            else
                                on_peers(peers);
                        });
                });
        });
}

void TrackerClient::schedule_retry(OnPeers on_peers, std::string host, uint16_t port) {
    if (!retry_enabled_ || retry_count_ >= MAX_RETRIES) return;
    retry_count_++;
    auto self = shared_from_this();
    auto timer = std::make_shared<asio::steady_timer>(io_, std::chrono::seconds(30));
    timer->async_wait([self, on_peers = std::move(on_peers), host = std::move(host), port](asio::error_code) mutable {
        self->do_announce(std::move(host), port, std::move(on_peers));
    });
}

} // namespace thinbt
