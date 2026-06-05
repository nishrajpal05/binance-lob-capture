#pragma once

#include "types.h"
#include <map>
#include <functional>
#include <cstring>

class OrderBook {
    std::map<int64_t, int64_t, std::greater<int64_t>> bids_;
    std::map<int64_t, int64_t> asks_;
    Top5Snapshot top5_;
    int64_t last_final_update_id_ = -1;
    bool synced_ = false;

    void rebuild_top5();

public:
    enum class ApplyResult { Ok, SequenceGap, FirstUpdate };

    ApplyResult apply_depth_diff(int64_t U, int64_t u, int64_t pu,
                                 const PriceLevel* bids, size_t nbids,
                                 const PriceLevel* asks, size_t nasks);

    void apply_depth5(const PriceLevel* bids, size_t nbids,
                      const PriceLevel* asks, size_t nasks);

    void reset();
    const Top5Snapshot& snapshot() const;
    bool is_synced() const;
};
