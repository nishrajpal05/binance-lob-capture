#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cinttypes>

struct Metrics {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> messages_processed{0};
    std::atomic<uint64_t> parse_errors{0};
    std::atomic<uint64_t> sequence_gaps{0};
    std::atomic<uint64_t> reconnects{0};
    std::atomic<uint64_t> md_rows_written{0};
    std::atomic<uint64_t> ob_rows_written{0};
    std::atomic<uint64_t> ring_full_count{0};

    void print_summary() const {
        fprintf(stderr, "\n--- Capture Metrics ---\n");
        fprintf(stderr, "Messages received:    %" PRIu64 "\n",
                messages_received.load(std::memory_order_relaxed));
        fprintf(stderr, "Messages processed:   %" PRIu64 "\n",
                messages_processed.load(std::memory_order_relaxed));
        fprintf(stderr, "Parse errors:         %" PRIu64 "\n",
                parse_errors.load(std::memory_order_relaxed));
        fprintf(stderr, "Sequence gaps:        %" PRIu64 "\n",
                sequence_gaps.load(std::memory_order_relaxed));
        fprintf(stderr, "Reconnects:           %" PRIu64 "\n",
                reconnects.load(std::memory_order_relaxed));
        fprintf(stderr, "Market data rows:     %" PRIu64 "\n",
                md_rows_written.load(std::memory_order_relaxed));
        fprintf(stderr, "Order book rows:      %" PRIu64 "\n",
                ob_rows_written.load(std::memory_order_relaxed));
        fprintf(stderr, "Ring buffer overflows: %" PRIu64 "\n",
                ring_full_count.load(std::memory_order_relaxed));
        fprintf(stderr, "---\n");
    }
};

inline Metrics g_metrics;
