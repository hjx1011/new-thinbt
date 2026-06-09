#ifndef THINBT_TRACKER_ACCEPTOR_HPP
#define THINBT_TRACKER_ACCEPTOR_HPP

#include <asio.hpp>
#include <memory>
#include <string>

namespace thinbt {

class TrackerServer;

class TrackerAcceptor {
public:
    TrackerAcceptor(asio::io_context& io, TrackerServer& server, uint16_t port);
    void start();

private:
    void do_accept();
    void handle_client(std::shared_ptr<asio::ip::tcp::socket> socket);

    asio::io_context& io_;
    TrackerServer& server_;
    asio::ip::tcp::acceptor acceptor_;
};

} // namespace thinbt
#endif
