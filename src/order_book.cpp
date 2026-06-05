#include "order_book.h"

OrderBook::ApplyResult OrderBook::apply_depth_diff(
    int64_t U, int64_t u, int64_t pu,
    const PriceLevel* bids, size_t nbids,
    const PriceLevel* asks, size_t nasks)
{
    (void)U;
    ApplyResult result;

    if (last_final_update_id_ < 0) {
        result = ApplyResult::FirstUpdate;
    } else if (pu >= 0 && pu != last_final_update_id_) {
        result = ApplyResult::SequenceGap;
    } else {
        result = ApplyResult::Ok;
    }

    for (size_t i = 0; i < nbids; ++i) {
        if (bids[i].qty == 0)
            bids_.erase(bids[i].price);
        else
            bids_[bids[i].price] = bids[i].qty;
    }

    for (size_t i = 0; i < nasks; ++i) {
        if (asks[i].qty == 0)
            asks_.erase(asks[i].price);
        else
            asks_[asks[i].price] = asks[i].qty;
    }

    last_final_update_id_ = u;
    synced_ = true;
    rebuild_top5();
    return result;
}

void OrderBook::apply_depth5(const PriceLevel* bids, size_t nbids,
                             const PriceLevel* asks, size_t nasks)
{
    top5_ = Top5Snapshot{};

    size_t bc = nbids < 5 ? nbids : 5;
    for (size_t i = 0; i < bc; ++i) {
        top5_.bid_prices[i] = bids[i].price;
        top5_.bid_sizes[i]  = bids[i].qty;
    }

    size_t ac = nasks < 5 ? nasks : 5;
    for (size_t i = 0; i < ac; ++i) {
        top5_.ask_prices[i] = asks[i].price;
        top5_.ask_sizes[i]  = asks[i].qty;
    }
}

void OrderBook::reset()
{
    bids_.clear();
    asks_.clear();
    top5_ = Top5Snapshot{};
    last_final_update_id_ = -1;
    synced_ = false;
}

void OrderBook::rebuild_top5()
{
    top5_ = Top5Snapshot{};

    size_t idx = 0;
    for (auto it = bids_.begin(); it != bids_.end() && idx < 5; ++it) {
        if (it->second > 0) {
            top5_.bid_prices[idx] = it->first;
            top5_.bid_sizes[idx]  = it->second;
            ++idx;
        }
    }

    idx = 0;
    for (auto it = asks_.begin(); it != asks_.end() && idx < 5; ++it) {
        if (it->second > 0) {
            top5_.ask_prices[idx] = it->first;
            top5_.ask_sizes[idx]  = it->second;
            ++idx;
        }
    }
}

const Top5Snapshot& OrderBook::snapshot() const
{
    return top5_;
}

bool OrderBook::is_synced() const
{
    return synced_;
}
