#include "tracker_client.hpp"
#include "yyjson.h"
#include <sstream>
#include <cstring>
#include <iostream>

namespace thinbt {
namespace {

// RAII wrapper, yyjson_doc 由调用方管理生命周期
struct JsonDoc {
    yyjson_doc* doc = nullptr;
    ~JsonDoc() { if (doc) yyjson_doc_free(doc); }
    explicit operator bool() const { return doc != nullptr; }
    yyjson_val* root() const { return yyjson_doc_get_root(doc); }
};

// 解析 Tracker announce 响应: {"interval":30,"peers":[...]}
std::vector<PexPeer> parse_tracker_response(const std::string& resp) {
    std::vector<PexPeer> peers;

    JsonDoc jd;
    jd.doc = yyjson_read(resp.data(), resp.size(), 0);
    if (!jd) return peers;

    auto* root = jd.root();
    if (!root || yyjson_obj_get(root, "error")) return peers;

    auto* peers_arr = yyjson_obj_get(root, "peers");
    if (!peers_arr || !yyjson_is_arr(peers_arr)) return peers;

    size_t count = yyjson_arr_size(peers_arr);
    for (size_t i = 0; i < count; i++) {
        auto* obj = yyjson_arr_get(peers_arr, i);
        if (!yyjson_is_obj(obj)) continue;

        auto* ip_val    = yyjson_obj_get(obj, "ip");
        auto* port_val  = yyjson_obj_get(obj, "port");
        if (!ip_val || !port_val) continue;

        auto* flags_val = yyjson_obj_get(obj, "flags");
        PexPeer p{};
        p.ip    = inet_addr(yyjson_get_str(ip_val));
        p.port  = htons(static_cast<uint16_t>(yyjson_get_uint(port_val)));
        p.flags = flags_val ? static_cast<uint8_t>(yyjson_get_uint(flags_val)) : 0;
        peers.push_back(p);
    }

    return peers;
}

} // anonymous namespace

TrackerClient::TrackerClient(asio::io_context& io, const std::string& info_hash_hex,
                              uint16_t p2p_port, uint32_t speed_mbps)
    : io_(io), info_hash_hex_(info_hash_hex), p2p_port_(p2p_port), speed_mbps_(speed_mbps) {}

void TrackerClient::announce(const std::string& host, uint16_t port, OnPeers on_peers) {
    retry_count_ = 0;
    dead_signalled_ = false;
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

                    // '\n' 分帧，使用 async_read_until 读完整行
                    auto resp_buf = std::make_shared<std::string>();
                    asio::async_read_until(*sock, asio::dynamic_buffer(*resp_buf), '\n',
                        [self, sock, resp_buf, on_peers = std::move(on_peers), host, port](asio::error_code ec3, size_t len) mutable {
                            if (ec3) { self->schedule_retry(std::move(on_peers), host, port); return; }

                            std::string resp(resp_buf->data(), len);
                            std::vector<PexPeer> peers = parse_tracker_response(resp);

                            on_peers(peers);
                            if (peers.empty()) {
                                self->schedule_empty_poll(on_peers, host, port);
                            }
                        });
                });
        });
}

void TrackerClient::schedule_empty_poll(OnPeers on_peers, std::string host, uint16_t port) {
    if (!retry_enabled_) return;
    auto self = shared_from_this();
    auto timer = std::make_shared<asio::steady_timer>(io_, std::chrono::milliseconds(EMPTY_POLL_INTERVAL_MS));
    timer->async_wait([self, timer, on_peers = std::move(on_peers), host = std::move(host), port](asio::error_code ec) mutable {
        if (ec) return;
        self->do_announce(std::move(host), port, std::move(on_peers));
    });
}

void TrackerClient::schedule_retry(OnPeers on_peers, std::string host, uint16_t port) {
    if (!retry_enabled_ || retry_count_ >= MAX_RETRIES) {
        // 重试耗尽，通知 TaskManager 将状态设为 waiting
        if (!dead_signalled_ && on_dead_) {
            dead_signalled_ = true;
            on_dead_();
        }
        return;
    }
    retry_count_++;
    auto self = shared_from_this();
    auto timer = std::make_shared<asio::steady_timer>(io_, std::chrono::seconds(30));
    timer->async_wait([self, timer, on_peers = std::move(on_peers), host = std::move(host), port](asio::error_code) mutable {
        self->do_announce(std::move(host), port, std::move(on_peers));
    });
}

} // namespace thinbt
