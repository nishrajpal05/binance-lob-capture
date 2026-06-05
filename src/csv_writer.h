#pragma once

#include "types.h"
#include <string>
#include <cstdio>

class MarketDataCsvWriter {
    FILE* file_;
    char write_buf_[262144];
    std::string row_buf_;

public:
    explicit MarketDataCsvWriter(const std::string& path);
    ~MarketDataCsvWriter();
    void write_row(const RawMessage& msg);
    void flush();
};

class OrderBookCsvWriter {
    FILE* file_;
    char write_buf_[262144];
    char row_buf_[4096];

public:
    explicit OrderBookCsvWriter(const std::string& path);
    ~OrderBookCsvWriter();
    void write_row(const Timestamp& ts, uint64_t seq_no, int32_t instrument_id,
                   char event_type, char side, const Top5Snapshot& snap);
    void flush();
};
