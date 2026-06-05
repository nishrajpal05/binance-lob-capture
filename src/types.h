#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include "ring_buffer.h"

constexpr int64_t PRICE_SCALE = 100'000'000LL;
constexpr int64_t QTY_SCALE   = 100'000'000LL;
constexpr size_t  MAX_PAYLOAD_SIZE = 16384;
constexpr size_t  PAYLOAD_PADDING  = 64;
constexpr size_t  MAX_SYMBOLS = 16;
constexpr size_t  MAX_LEVELS  = 200;
constexpr size_t  RING_CAPACITY = 8192;

struct Timestamp {
    int64_t sec  = 0;
    int32_t nsec = 0;
};

enum class Venue : uint8_t { Spot = 0, UsdM = 1 };
enum class StreamKind : uint8_t { DepthDiff = 0, Depth5 = 1, Trade = 2 };

inline const char* venue_str(Venue v) {
    switch (v) {
        case Venue::Spot: return "spot";
        case Venue::UsdM: return "usdm";
    }
    return "unknown";
}

inline const char* stream_kind_str(StreamKind k) {
    switch (k) {
        case StreamKind::DepthDiff: return "depth_diff";
        case StreamKind::Depth5:    return "depth5";
        case StreamKind::Trade:     return "trade";
    }
    return "unknown";
}

struct PriceLevel {
    int64_t price = 0;
    int64_t qty   = 0;
};

struct Top5Snapshot {
    int64_t bid_prices[5] = {};
    int64_t bid_sizes[5]  = {};
    int64_t ask_prices[5] = {};
    int64_t ask_sizes[5]  = {};
};

struct RawMessage {
    Timestamp  recv_ts;
    Venue      venue;
    StreamKind kind;
    uint32_t   shard_id;
    uint32_t   conn_epoch;
    uint64_t   conn_seq;
    char       symbol[32];
    size_t     symbol_len;
    char       payload[MAX_PAYLOAD_SIZE + PAYLOAD_PADDING];
    size_t     payload_len;
};

using RingBufferType = SPSCRingBuffer<RawMessage, RING_CAPACITY>;
