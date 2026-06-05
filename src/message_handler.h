#pragma once
#include "types.h"
#include "order_book.h"
#include "csv_writer.h"
#include "simdjson.h"
#include <atomic>
#include <unordered_map>
#include <string>

class MessageHandler {
public:
    MessageHandler(RingBufferType& ring, const std::string& md_csv_path,
                   const std::string& ob_csv_path, std::atomic<bool>& running);
    void run();

private:
    RingBufferType& ring_;
    std::atomic<bool>& running_;
    MarketDataCsvWriter md_writer_;
    OrderBookCsvWriter ob_writer_;
    std::unordered_map<std::string, OrderBook> books_;
    std::unordered_map<std::string, int32_t> symbol_ids_;
    uint64_t seq_no_ = 0;
    int32_t next_symbol_id_ = 0;
    PriceLevel bid_levels_[MAX_LEVELS];
    PriceLevel ask_levels_[MAX_LEVELS];
    simdjson::dom::parser parser_;

    void process_message(const RawMessage& msg);
    void process_depth_diff(const RawMessage& msg);
    void process_depth5(const RawMessage& msg);
    void process_trade(const RawMessage& msg);
    int32_t get_or_assign_symbol_id(const std::string& symbol);
};
