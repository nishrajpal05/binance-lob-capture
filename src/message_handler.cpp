#include "message_handler.h"
#include "fixed_point.h"
#include "metrics.h"
#include <thread>
#include <chrono>
#include <cstring>

MessageHandler::MessageHandler(RingBufferType& ring, const std::string& md_csv_path,
                               const std::string& ob_csv_path, std::atomic<bool>& running)
    : ring_(ring)
    , running_(running)
    , md_writer_(md_csv_path)
    , ob_writer_(ob_csv_path)
{
}

void MessageHandler::run() {
    int idle_count = 0;
    while (running_.load(std::memory_order_relaxed)) {
        auto* msg = ring_.try_pop();
        if (msg) {
            idle_count = 0;
            process_message(*msg);
            ring_.commit_pop();
        } else {
            if (++idle_count > 100)
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            else
                std::this_thread::yield();
        }
    }
    md_writer_.flush();
    ob_writer_.flush();
}

void MessageHandler::process_message(const RawMessage& msg) {
    md_writer_.write_row(msg);
    switch (msg.kind) {
        case StreamKind::DepthDiff: process_depth_diff(msg); break;
        case StreamKind::Depth5:    process_depth5(msg);     break;
        case StreamKind::Trade:     process_trade(msg);      break;
    }
    g_metrics.messages_processed.fetch_add(1, std::memory_order_relaxed);
}

void MessageHandler::process_depth_diff(const RawMessage& msg) {
    simdjson::padded_string_view padded(msg.payload, msg.payload_len,
                                        msg.payload_len + PAYLOAD_PADDING);
    auto doc_result = parser_.parse(padded);
    if (doc_result.error()) {
        g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    auto doc = doc_result.value();

    int64_t U = 0, u = 0, pu = -1;
    (void)doc["U"].get(U);
    (void)doc["u"].get(u);
    int64_t pu_val;
    if (!doc["pu"].get(pu_val))
        pu = pu_val;

    size_t nbids = 0;
    simdjson::dom::array bids_arr;
    if (!doc["b"].get(bids_arr)) {
        for (auto level : bids_arr) {
            simdjson::dom::array pair;
            if (level.get(pair)) continue;
            auto it = pair.begin();
            std::string_view price_sv, qty_sv;
            if ((*it).get(price_sv)) continue;
            ++it;
            if ((*it).get(qty_sv)) continue;
            bid_levels_[nbids].price = fp::parse_decimal(price_sv.data(), price_sv.size());
            bid_levels_[nbids].qty = fp::parse_decimal(qty_sv.data(), qty_sv.size());
            if (++nbids >= MAX_LEVELS) break;
        }
    }

    size_t nasks = 0;
    simdjson::dom::array asks_arr;
    if (!doc["a"].get(asks_arr)) {
        for (auto level : asks_arr) {
            simdjson::dom::array pair;
            if (level.get(pair)) continue;
            auto it = pair.begin();
            std::string_view price_sv, qty_sv;
            if ((*it).get(price_sv)) continue;
            ++it;
            if ((*it).get(qty_sv)) continue;
            ask_levels_[nasks].price = fp::parse_decimal(price_sv.data(), price_sv.size());
            ask_levels_[nasks].qty = fp::parse_decimal(qty_sv.data(), qty_sv.size());
            if (++nasks >= MAX_LEVELS) break;
        }
    }

    std::string sym(msg.symbol, msg.symbol_len);
    auto& book = books_[sym];
    auto result = book.apply_depth_diff(U, u, pu, bid_levels_, nbids, ask_levels_, nasks);
    if (result == OrderBook::ApplyResult::SequenceGap)
        g_metrics.sequence_gaps.fetch_add(1, std::memory_order_relaxed);

    int32_t sym_id = get_or_assign_symbol_id(sym);
    ob_writer_.write_row(msg.recv_ts, ++seq_no_, sym_id, 'D', 'N', book.snapshot());
}

void MessageHandler::process_depth5(const RawMessage& msg) {
    simdjson::padded_string_view padded(msg.payload, msg.payload_len,
                                        msg.payload_len + PAYLOAD_PADDING);
    auto doc_result = parser_.parse(padded);
    if (doc_result.error()) {
        g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    auto doc = doc_result.value();

    size_t nbids = 0;
    simdjson::dom::array bids_arr;
    if (doc["bids"].get(bids_arr) != 0)
        (void)doc["b"].get(bids_arr);
    for (auto level : bids_arr) {
        simdjson::dom::array pair;
        if (level.get(pair)) continue;
        auto it = pair.begin();
        std::string_view price_sv, qty_sv;
        if ((*it).get(price_sv)) continue;
        ++it;
        if ((*it).get(qty_sv)) continue;
        bid_levels_[nbids].price = fp::parse_decimal(price_sv.data(), price_sv.size());
        bid_levels_[nbids].qty = fp::parse_decimal(qty_sv.data(), qty_sv.size());
        if (++nbids >= MAX_LEVELS) break;
    }

    size_t nasks = 0;
    simdjson::dom::array asks_arr;
    if (doc["asks"].get(asks_arr) != 0)
        (void)doc["a"].get(asks_arr);
    for (auto level : asks_arr) {
        simdjson::dom::array pair;
        if (level.get(pair)) continue;
        auto it = pair.begin();
        std::string_view price_sv, qty_sv;
        if ((*it).get(price_sv)) continue;
        ++it;
        if ((*it).get(qty_sv)) continue;
        ask_levels_[nasks].price = fp::parse_decimal(price_sv.data(), price_sv.size());
        ask_levels_[nasks].qty = fp::parse_decimal(qty_sv.data(), qty_sv.size());
        if (++nasks >= MAX_LEVELS) break;
    }

    std::string sym(msg.symbol, msg.symbol_len);
    auto& book = books_[sym];
    book.apply_depth5(bid_levels_, nbids, ask_levels_, nasks);
    int32_t sym_id = get_or_assign_symbol_id(sym);
    ob_writer_.write_row(msg.recv_ts, ++seq_no_, sym_id, 'F', 'N', book.snapshot());
}

void MessageHandler::process_trade(const RawMessage& msg) {
    std::string sym(msg.symbol, msg.symbol_len);
    char side = 'N';

    simdjson::padded_string_view padded(msg.payload, msg.payload_len,
                                        msg.payload_len + PAYLOAD_PADDING);
    auto doc_result = parser_.parse(padded);
    if (!doc_result.error()) {
        bool is_buyer_maker = false;
        (void)doc_result.value()["m"].get(is_buyer_maker);
        side = is_buyer_maker ? 'S' : 'B';
    }

    int32_t sym_id = get_or_assign_symbol_id(sym);
    auto& book = books_[sym];
    ob_writer_.write_row(msg.recv_ts, ++seq_no_, sym_id, 'T', side, book.snapshot());
}

int32_t MessageHandler::get_or_assign_symbol_id(const std::string& symbol) {
    auto it = symbol_ids_.find(symbol);
    if (it != symbol_ids_.end())
        return it->second;
    int32_t id = next_symbol_id_++;
    symbol_ids_[symbol] = id;
    return id;
}
