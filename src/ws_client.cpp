#include "ws_client.h"
#include "metrics.h"
#include <cstring>
#include <cctype>

static void* my_memmem(const void* haystack, size_t hlen, const void* needle, size_t nlen) {
    if (nlen > hlen) return nullptr;
    const char* h = (const char*)haystack;
    const char* n = (const char*)needle;
    for (size_t i = 0; i <= hlen - nlen; i++) {
        if (memcmp(h + i, n, nlen) == 0) return (void*)(h + i);
    }
    return nullptr;
}

WebSocketClient::WebSocketClient(net::io_context& ioc, RingBufferType& ring, Venue venue,
                                 const std::vector<std::string>& symbols, uint32_t shard_id,
                                 std::atomic<bool>& running)
    : ioc_(ioc)
    , ssl_ctx_(ssl::context::tlsv12_client)
    , resolver_(net::make_strand(ioc_))
    , reconnect_timer_(ioc_)
    , ring_(ring)
    , venue_(venue)
    , symbols_(symbols)
    , shard_id_(shard_id)
    , running_(running) {
    ssl_ctx_.set_default_verify_paths();
    ssl_ctx_.set_verify_mode(ssl::verify_peer);
    build_url();
}

void WebSocketClient::build_url() {
    if (venue_ == Venue::Spot) {
        host_ = "stream.binance.com";
        port_ = "9443";
    } else {
        host_ = "fstream.binance.com";
        port_ = "443";
    }

    if (venue_ == Venue::Spot)
        path_ = "/stream?streams=";
    else
        path_ = "/public/stream?streams=";

    for (size_t i = 0; i < symbols_.size(); i++) {
        std::string sym;
        sym.reserve(symbols_[i].size());
        for (char c : symbols_[i]) {
            sym += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        if (i > 0) path_ += '/';
        path_ += sym + "@depth@100ms/" + sym + "@depth5@100ms/" + sym + "@trade";
    }
}

void WebSocketClient::start() {
    do_resolve();
}

void WebSocketClient::do_resolve() {
    if (!running_) return;
    resolver_.async_resolve(host_, port_,
        [self = shared_from_this()](beast::error_code ec, tcp::resolver::results_type results) {
            self->on_resolve(ec, std::move(results));
        });
}

void WebSocketClient::on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec || !running_) {
        fprintf(stderr, "[ws] resolve error: %s\n", ec.message().c_str());
        schedule_reconnect();
        return;
    }

    ws_ = std::make_unique<ws::stream<beast::ssl_stream<beast::tcp_stream>>>(
        net::make_strand(ioc_), ssl_ctx_);

    beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(30));
    beast::get_lowest_layer(*ws_).async_connect(results,
        [self = shared_from_this()](beast::error_code ec, tcp::resolver::results_type::endpoint_type ep) {
            self->on_connect(ec, ep);
        });
}

void WebSocketClient::on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
    if (ec || !running_) {
        schedule_reconnect();
        return;
    }

    SSL_set_tlsext_host_name(ws_->next_layer().native_handle(), host_.c_str());
    beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(30));
    ws_->next_layer().async_handshake(ssl::stream_base::client,
        [self = shared_from_this()](beast::error_code ec) {
            self->on_ssl_handshake(ec);
        });
}

void WebSocketClient::on_ssl_handshake(beast::error_code ec) {
    if (ec || !running_) {
        schedule_reconnect();
        return;
    }

    beast::get_lowest_layer(*ws_).expires_never();
    ws_->set_option(ws::stream_base::timeout::suggested(beast::role_type::client));
    ws_->set_option(ws::stream_base::decorator([](ws::request_type& req) {
        req.set(beast::http::field::user_agent, "binance-capture/1.0");
    }));

    ws_->async_handshake(host_ + ":" + port_, path_,
        [self = shared_from_this()](beast::error_code ec) {
            self->on_ws_handshake(ec);
        });
}

void WebSocketClient::on_ws_handshake(beast::error_code ec) {
    if (ec || !running_) {
        schedule_reconnect();
        return;
    }

    if (connected_once_) conn_epoch_++;
    connected_once_ = true;
    conn_seq_ = 0;
    reconnect_delay_ms_ = 1000;
    g_metrics.reconnects.fetch_add(1, std::memory_order_relaxed);
    fprintf(stderr, "[ws] connected to %s epoch=%u\n", host_.c_str(), conn_epoch_);
    do_read();
}

void WebSocketClient::do_read() {
    if (!running_) {
        do_close();
        return;
    }

    ws_->async_read(read_buf_,
        [self = shared_from_this()](beast::error_code ec, size_t bytes) {
            self->on_read(ec, bytes);
        });
}

void WebSocketClient::on_read(beast::error_code ec, size_t) {
    if (ec) {
        if (!running_ || ec == ws::error::closed) {
            do_close();
        } else {
            fprintf(stderr, "[ws] read error: %s\n", ec.message().c_str());
            schedule_reconnect();
        }
        return;
    }

    auto data = static_cast<const char*>(read_buf_.data().data());
    auto size = read_buf_.data().size();
    handle_message(data, size);
    read_buf_.consume(read_buf_.size());
    do_read();
}

void WebSocketClient::handle_message(const char* data, size_t len) {
    g_metrics.messages_received.fetch_add(1, std::memory_order_relaxed);

    const char* stream_key = "\"stream\":\"";
    auto* p = static_cast<const char*>(my_memmem(data, len, stream_key, 10));
    if (!p) return;

    const char* name_start = p + 10;
    const char* name_end = static_cast<const char*>(
        memchr(name_start, '"', data + len - name_start));
    if (!name_end) return;
    size_t name_len = name_end - name_start;

    size_t at_pos = 0;
    while (at_pos < name_len && name_start[at_pos] != '@') at_pos++;

    char sym_upper[32];
    for (size_t i = 0; i < at_pos && i < 31; i++)
        sym_upper[i] = std::toupper(static_cast<unsigned char>(name_start[i]));
    sym_upper[std::min(at_pos, (size_t)31)] = 0;

    const char* data_key = "\"data\":";
    auto* dp = static_cast<const char*>(my_memmem(data, len, data_key, 7));
    if (!dp) return;

    const char* payload_start = dp + 7;
    size_t payload_len = (data + len - 1) - payload_start;

    auto now = std::chrono::system_clock::now();
    auto epoch = now.time_since_epoch();
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(epoch);
    auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch)
               - std::chrono::duration_cast<std::chrono::nanoseconds>(secs);

    auto* slot = ring_.try_push();
    if (!slot) {
        g_metrics.ring_full_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    slot->recv_ts.sec = secs.count();
    slot->recv_ts.nsec = static_cast<int32_t>(nsecs.count());
    slot->venue = venue_;
    slot->kind = classify_stream(name_start, name_len);
    slot->shard_id = shard_id_;
    slot->conn_epoch = conn_epoch_;
    slot->conn_seq = conn_seq_++;

    size_t sym_copy = std::min(at_pos, sizeof(slot->symbol) - 1);
    std::memcpy(slot->symbol, sym_upper, sym_copy);
    slot->symbol_len = sym_copy;

    size_t pay_copy = std::min(payload_len, (size_t)MAX_PAYLOAD_SIZE);
    std::memcpy(slot->payload, payload_start, pay_copy);
    std::memset(slot->payload + pay_copy, 0, PAYLOAD_PADDING);
    slot->payload_len = pay_copy;

    ring_.commit_push();
}

void WebSocketClient::schedule_reconnect() {
    if (!running_) return;
    fprintf(stderr, "[ws] reconnecting in %dms...\n", reconnect_delay_ms_);
    reconnect_timer_.expires_after(std::chrono::milliseconds(reconnect_delay_ms_));
    reconnect_delay_ms_ = std::min(reconnect_delay_ms_ * 2, 30000);
    reconnect_timer_.async_wait(
        [self = shared_from_this()](beast::error_code ec) {
            if (!ec && self->running_) self->do_resolve();
        });
}

void WebSocketClient::do_close() {
    if (ws_ && ws_->is_open()) {
        beast::error_code ec;
        ws_->close(ws::close_code::normal, ec);
    }
}

void WebSocketClient::stop() {
    net::post(ioc_, [self = shared_from_this()] {
        self->do_close();
        self->reconnect_timer_.cancel();
    });
}

StreamKind WebSocketClient::classify_stream(const char* name, size_t len) {
    if (my_memmem(name, len, "@depth5", 7))
        return StreamKind::Depth5;
    if (my_memmem(name, len, "@depth@", 7))
        return StreamKind::DepthDiff;
    if (my_memmem(name, len, "@trade", 6))
        return StreamKind::Trade;
    return StreamKind::DepthDiff;
}
