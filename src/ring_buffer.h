#pragma once

#include <atomic>
#include <cstddef>

template<typename T, size_t N>
class SPSCRingBuffer {
    static_assert((N & (N - 1)) == 0, "capacity must be power of two");

    static constexpr size_t MASK = N - 1;

    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) T slots_[N];

public:
    T* try_push() {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t next = (h + 1) & MASK;
        if (next == tail_.load(std::memory_order_acquire))
            return nullptr;
        return &slots_[h];
    }

    void commit_push() {
        size_t h = head_.load(std::memory_order_relaxed);
        head_.store((h + 1) & MASK, std::memory_order_release);
    }

    T* try_pop() {
        size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_.load(std::memory_order_acquire))
            return nullptr;
        return &slots_[t];
    }

    void commit_pop() {
        size_t t = tail_.load(std::memory_order_relaxed);
        tail_.store((t + 1) & MASK, std::memory_order_release);
    }

    size_t size() const {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t t = tail_.load(std::memory_order_relaxed);
        return (h - t) & MASK;
    }
};
