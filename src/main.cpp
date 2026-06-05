#include "types.h"
#include "ws_client.h"
#include "message_handler.h"
#include "replay.h"
#include "metrics.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <atomic>
#include <csignal>
#include <memory>
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <ctime>
#include <boost/asio.hpp>
namespace net = boost::asio;
namespace fs = std::filesystem;

static std::atomic<bool> g_running{true};
static net::io_context* g_ioc_ptr = nullptr;

static void on_signal(int) {
    g_running.store(false, std::memory_order_relaxed);
    if (g_ioc_ptr)
        g_ioc_ptr->stop();
}

int main(int argc, char** argv) {
    std::string venue_arg = "spot";
    std::string symbols_arg = "BTCUSDT";
    std::string output_dir = "./output";
    int duration = 0;
    std::string replay_path;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--venue" && i + 1 < argc)
            venue_arg = argv[++i];
        else if (a == "--symbols" && i + 1 < argc)
            symbols_arg = argv[++i];
        else if (a == "--output-dir" && i + 1 < argc)
            output_dir = argv[++i];
        else if (a == "--duration" && i + 1 < argc)
            duration = std::stoi(argv[++i]);
        else if (a == "--replay" && i + 1 < argc)
            replay_path = argv[++i];
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    if (!replay_path.empty()) {
        run_replay(replay_path, output_dir);
        g_metrics.print_summary();
        return 0;
    }

    Venue venue = (venue_arg == "usdm") ? Venue::UsdM : Venue::Spot;

    std::vector<std::string> symbols;
    std::istringstream ss(symbols_arg);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        std::transform(tok.begin(), tok.end(), tok.begin(), ::toupper);
        symbols.push_back(tok);
    }

    fs::create_directories(output_dir);

    auto now_tp = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now_tp);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &tt);
#else
    gmtime_r(&tt, &tm);
#endif
    char date_str[16];
    std::strftime(date_str, sizeof(date_str), "%Y-%m-%d", &tm);

    std::string base = output_dir + "/market_data_" + venue_arg + "_" + symbols[0] + "_" + date_str;
    std::string md_path = base + ".csv";
    std::string ob_path = base + "_orderbook.csv";

    auto ring = std::make_unique<RingBufferType>();

    MessageHandler handler(*ring, md_path, ob_path, g_running);
    std::thread consumer([&handler]() { handler.run(); });

    net::io_context ioc;
    g_ioc_ptr = &ioc;

    auto client = std::make_shared<WebSocketClient>(ioc, *ring, venue, symbols, 0, g_running);
    client->start();

    std::unique_ptr<net::steady_timer> timer;
    if (duration > 0) {
        timer = std::make_unique<net::steady_timer>(ioc, std::chrono::seconds(duration));
        timer->async_wait([](const boost::system::error_code&) {
            g_running.store(false, std::memory_order_relaxed);
            if (g_ioc_ptr)
                g_ioc_ptr->stop();
        });
    }

    fprintf(stderr, "Starting capture: venue=%s symbols=%s output=%s\n",
            venue_arg.c_str(), symbols_arg.c_str(), output_dir.c_str());

    ioc.run();

    g_running.store(false, std::memory_order_relaxed);
    consumer.join();
    g_metrics.print_summary();
    return 0;
}
