#include "replay.h"
#include "types.h"
#include "order_book.h"
#include "csv_writer.h"
#include "fixed_point.h"
#include "metrics.h"
#include "simdjson.h"
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <filesystem>

namespace fs = std::filesystem;

static std::vector<std::string> parse_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    size_t i = 0;
    size_t len = line.size();

    while (i < len) {
        std::string field;
        if (i < len && line[i] == '"') {
            ++i;
            while (i < len) {
                if (line[i] == '"') {
                    if (i + 1 < len && line[i + 1] == '"') {
                        field += '"';
                        i += 2;
                    } else {
                        ++i;
                        break;
                    }
                } else {
                    field += line[i];
                    ++i;
                }
            }
            if (i < len && line[i] == ',')
                ++i;
        } else {
            size_t start = i;
            while (i < len && line[i] != ',')
                ++i;
            field = line.substr(start, i - start);
            if (i < len)
                ++i;
        }
        fields.push_back(std::move(field));
    }
    return fields;
}

static StreamKind parse_stream_kind(const std::string& s) {
    if (s == "depth_diff" || s == "DepthDiff")
        return StreamKind::DepthDiff;
    if (s == "depth5" || s == "Depth5")
        return StreamKind::Depth5;
    return StreamKind::Trade;
}

static size_t parse_levels(simdjson::dom::array& arr, PriceLevel* levels, size_t max_levels) {
    size_t count = 0;
    for (auto level : arr) {
        simdjson::dom::array pair;
        if (level.get(pair)) continue;
        auto it = pair.begin();
        std::string_view price_sv, qty_sv;
        if ((*it).get(price_sv)) continue;
        ++it;
        if ((*it).get(qty_sv)) continue;
        levels[count].price = fp::parse_decimal(price_sv.data(), price_sv.size());
        levels[count].qty = fp::parse_decimal(qty_sv.data(), qty_sv.size());
        if (++count >= max_levels) break;
    }
    return count;
}

void run_replay(const std::string& input_csv, const std::string& output_dir) {
    std::ifstream ifs(input_csv);
    if (!ifs.is_open()) {
        fprintf(stderr, "Failed to open input file: %s\n", input_csv.c_str());
        return;
    }

    std::string header;
    std::getline(ifs, header);

    fs::path input_path(input_csv);
    std::string stem = input_path.stem().string();
    std::string ob_filename = stem + "_orderbook.csv";
    fs::path ob_path = fs::path(output_dir) / ob_filename;
    fs::create_directories(output_dir);

    OrderBookCsvWriter ob_writer(ob_path.string());
    std::unordered_map<std::string, OrderBook> books;
    std::unordered_map<std::string, int32_t> symbol_ids;
    simdjson::dom::parser parser;
    PriceLevel bid_levels[MAX_LEVELS];
    PriceLevel ask_levels[MAX_LEVELS];
    uint64_t seq_no = 0;
    int32_t next_sym_id = 0;
    uint64_t rows_processed = 0;

    auto get_or_assign_id = [&](const std::string& sym) -> int32_t {
        auto it = symbol_ids.find(sym);
        if (it != symbol_ids.end()) return it->second;
        int32_t id = next_sym_id++;
        symbol_ids[sym] = id;
        return id;
    };

    std::string line;
    simdjson::padded_string padded_buf;

    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        auto fields = parse_csv_line(line);
        if (fields.size() < 9) continue;

        int64_t tsec = std::stoll(fields[0]);
        int32_t tnsec = std::stoi(fields[1]);
        StreamKind kind = parse_stream_kind(fields[3]);
        std::string symbol = fields[7];
        std::string& payload = fields[8];

        Timestamp ts{tsec, tnsec};

        padded_buf = simdjson::padded_string(payload.data(), payload.size());
        auto doc_result = parser.parse(padded_buf);
        if (doc_result.error()) {
            g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        auto doc = doc_result.value();

        int32_t sym_id = get_or_assign_id(symbol);
        auto& book = books[symbol];

        if (kind == StreamKind::DepthDiff) {
            int64_t U = 0, u = 0, pu = -1;
            (void)doc["U"].get(U);
            (void)doc["u"].get(u);
            int64_t pu_val;
            if (!doc["pu"].get(pu_val))
                pu = pu_val;

            size_t nbids = 0;
            simdjson::dom::array bids_arr;
            if (!doc["b"].get(bids_arr))
                nbids = parse_levels(bids_arr, bid_levels, MAX_LEVELS);

            size_t nasks = 0;
            simdjson::dom::array asks_arr;
            if (!doc["a"].get(asks_arr))
                nasks = parse_levels(asks_arr, ask_levels, MAX_LEVELS);

            auto result = book.apply_depth_diff(U, u, pu, bid_levels, nbids, ask_levels, nasks);
            if (result == OrderBook::ApplyResult::SequenceGap)
                g_metrics.sequence_gaps.fetch_add(1, std::memory_order_relaxed);

            ob_writer.write_row(ts, ++seq_no, sym_id, 'D', 'N', book.snapshot());
        } else if (kind == StreamKind::Depth5) {
            simdjson::dom::array bids_arr;
            if (doc["bids"].get(bids_arr) != 0)
                (void)doc["b"].get(bids_arr);
            size_t nbids = parse_levels(bids_arr, bid_levels, MAX_LEVELS);

            simdjson::dom::array asks_arr;
            if (doc["asks"].get(asks_arr) != 0)
                (void)doc["a"].get(asks_arr);
            size_t nasks = parse_levels(asks_arr, ask_levels, MAX_LEVELS);

            book.apply_depth5(bid_levels, nbids, ask_levels, nasks);
            ob_writer.write_row(ts, ++seq_no, sym_id, 'F', 'N', book.snapshot());
        } else if (kind == StreamKind::Trade) {
            char side = 'N';
            bool is_buyer_maker = false;
            if (!doc["m"].get(is_buyer_maker))
                side = is_buyer_maker ? 'S' : 'B';

            ob_writer.write_row(ts, ++seq_no, sym_id, 'T', side, book.snapshot());
        }

        ++rows_processed;
    }

    ob_writer.flush();
    fprintf(stderr, "Replay complete: %llu rows\n", static_cast<unsigned long long>(rows_processed));
}
