#pragma once
#include "types.h"
#include <string>
#include <vector>
#include <atomic>
#include <memory>
#include <algorithm>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

namespace net = boost::asio;
namespace ssl = net::ssl;
namespace beast = boost::beast;
namespace ws = beast::websocket;
using tcp = net::ip::tcp;

class WebSocketClient : public std::enable_shared_from_this<WebSocketClient> {
public:
    WebSocketClient(net::io_context& ioc, RingBufferType& ring, Venue venue,
                    const std::vector<std::string>& symbols, uint32_t shard_id,
                    std::atomic<bool>& running);
    void start();
    void stop();

private:
    net::io_context& ioc_;
    ssl::context ssl_ctx_;
    std::unique_ptr<ws::stream<beast::ssl_stream<beast::tcp_stream>>> ws_;
    tcp::resolver resolver_;
    beast::flat_buffer read_buf_;
    net::steady_timer reconnect_timer_;

    RingBufferType& ring_;
    Venue venue_;
    std::vector<std::string> symbols_;
    uint32_t shard_id_;
    std::atomic<bool>& running_;

    std::string host_;
    std::string port_;
    std::string path_;
    uint32_t conn_epoch_ = 0;
    uint64_t conn_seq_ = 0;
    int reconnect_delay_ms_ = 1000;
    bool connected_once_ = false;

    void build_url();
    void do_resolve();
    void on_resolve(beast::error_code ec, tcp::resolver::results_type results);
    void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep);
    void on_ssl_handshake(beast::error_code ec);
    void on_ws_handshake(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, size_t bytes);
    void handle_message(const char* data, size_t len);
    void do_close();
    void schedule_reconnect();
    StreamKind classify_stream(const char* name, size_t len);
};
