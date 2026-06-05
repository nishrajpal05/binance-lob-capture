#include "csv_writer.h"
#include "fixed_point.h"
#include "metrics.h"
#include <cstring>
#include <cinttypes>
#include <algorithm>

MarketDataCsvWriter::MarketDataCsvWriter(const std::string& path)
    : file_(std::fopen(path.c_str(), "wb"))
{
    std::setvbuf(file_, write_buf_, _IOFBF, sizeof(write_buf_));
    const char header[] = "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json\n";
    std::fwrite(header, 1, sizeof(header) - 1, file_);
    row_buf_.reserve(32768);
}

MarketDataCsvWriter::~MarketDataCsvWriter()
{
    if (file_) {
        std::fflush(file_);
        std::fclose(file_);
    }
}

void MarketDataCsvWriter::write_row(const RawMessage& msg)
{
    row_buf_.clear();
    char tmp[64];

    int n = std::snprintf(tmp, sizeof(tmp), "%" PRId64, msg.recv_ts.sec);
    row_buf_.append(tmp, n);
    row_buf_ += ',';

    n = std::snprintf(tmp, sizeof(tmp), "%" PRId32, msg.recv_ts.nsec);
    row_buf_.append(tmp, n);
    row_buf_ += ',';

    row_buf_.append(venue_str(msg.venue));
    row_buf_ += ',';

    row_buf_.append(stream_kind_str(msg.kind));
    row_buf_ += ',';

    n = std::snprintf(tmp, sizeof(tmp), "%" PRIu32, msg.shard_id);
    row_buf_.append(tmp, n);
    row_buf_ += ',';

    n = std::snprintf(tmp, sizeof(tmp), "%" PRIu32, msg.conn_epoch);
    row_buf_.append(tmp, n);
    row_buf_ += ',';

    n = std::snprintf(tmp, sizeof(tmp), "%" PRIu64, msg.conn_seq);
    row_buf_.append(tmp, n);
    row_buf_ += ',';

    row_buf_.append(msg.symbol, msg.symbol_len);
    row_buf_ += ',';

    const char* payload = msg.payload;
    size_t plen = msg.payload_len;

    bool needs_quoting = false;
    for (size_t i = 0; i < plen; ++i) {
        char c = payload[i];
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            needs_quoting = true;
            break;
        }
    }

    if (needs_quoting) {
        row_buf_ += '"';
        for (size_t i = 0; i < plen; ++i) {
            if (payload[i] == '"')
                row_buf_ += "\"\"";
            else
                row_buf_ += payload[i];
        }
        row_buf_ += '"';
    } else {
        row_buf_.append(payload, plen);
    }

    row_buf_ += '\n';
    std::fwrite(row_buf_.data(), 1, row_buf_.size(), file_);
    g_metrics.md_rows_written.fetch_add(1, std::memory_order_relaxed);
}

void MarketDataCsvWriter::flush()
{
    std::fflush(file_);
}

OrderBookCsvWriter::OrderBookCsvWriter(const std::string& path)
    : file_(std::fopen(path.c_str(), "wb"))
{
    std::setvbuf(file_, write_buf_, _IOFBF, sizeof(write_buf_));
    const char header[] =
        "tsec,tnsec,seqNo,id,type,side,"
        "bid0,bid1,bid2,bid3,bid4,"
        "bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,"
        "ask0,ask1,ask2,ask3,ask4,"
        "ask_size0,ask_size1,ask_size2,ask_size3,ask_size4\n";
    std::fwrite(header, 1, sizeof(header) - 1, file_);
}

OrderBookCsvWriter::~OrderBookCsvWriter()
{
    if (file_) {
        std::fflush(file_);
        std::fclose(file_);
    }
}

void OrderBookCsvWriter::write_row(const Timestamp& ts, uint64_t seq_no,
                                   int32_t instrument_id, char event_type,
                                   char side, const Top5Snapshot& snap)
{
    int len = std::snprintf(row_buf_, sizeof(row_buf_),
        "%" PRId64 ",%" PRId32 ",%" PRIu64 ",%" PRId32 ",%c,%c,"
        "%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ","
        "%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ","
        "%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ","
        "%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 "\n",
        ts.sec, ts.nsec, seq_no, instrument_id, event_type, side,
        snap.bid_prices[0], snap.bid_prices[1], snap.bid_prices[2],
        snap.bid_prices[3], snap.bid_prices[4],
        snap.bid_sizes[0], snap.bid_sizes[1], snap.bid_sizes[2],
        snap.bid_sizes[3], snap.bid_sizes[4],
        snap.ask_prices[0], snap.ask_prices[1], snap.ask_prices[2],
        snap.ask_prices[3], snap.ask_prices[4],
        snap.ask_sizes[0], snap.ask_sizes[1], snap.ask_sizes[2],
        snap.ask_sizes[3], snap.ask_sizes[4]);

    std::fwrite(row_buf_, 1, len, file_);
    g_metrics.ob_rows_written.fetch_add(1, std::memory_order_relaxed);
}

void OrderBookCsvWriter::flush()
{
    std::fflush(file_);
}
