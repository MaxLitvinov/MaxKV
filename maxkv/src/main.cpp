#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include "kv/engine.hpp"
#include "kv/server.hpp"
#include "kv/store.hpp"
#include "kv/wal.hpp"

static kv::Server* g_server = nullptr;
static void on_signal(int) {
    if (g_server) g_server->stop();
}

static void usage() {
    std::puts(
        "MaxKV - Redis-compatible in-memory key-value server\n\n"
        "Usage: maxkv [options]\n"
        "  --host <addr>     bind address        (default 127.0.0.1)\n"
        "  --port <port>     TCP port            (default 6380)\n"
        "  --threads <n>     I/O worker threads  (default: hardware concurrency)\n"
        "  --aof <path>      append-only file    (default maxkv.aof)\n"
        "  --no-aof          disable persistence\n");
}

int main(int argc, char** argv) {
    std::string host = "127.0.0.1", aof = "maxkv.aof";
    int port = 6380;
    unsigned threads = std::thread::hardware_concurrency();
    if (threads == 0) threads = 2;
    bool use_aof = true;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* { return i + 1 < argc ? argv[++i] : (usage(), std::exit(2), nullptr); };
        if (a == "--host") host = next();
        else if (a == "--port") port = std::atoi(next());
        else if (a == "--threads") threads = static_cast<unsigned>(std::atoi(next()));
        else if (a == "--aof") aof = next();
        else if (a == "--no-aof") use_aof = false;
        else { usage(); return a == "--help" ? 0 : 2; }
    }

    std::signal(SIGPIPE, SIG_IGN);
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    kv::Store store;
    std::unique_ptr<kv::Wal> wal;
    if (use_aof) wal = std::make_unique<kv::Wal>(aof);
    kv::Engine engine(store, wal.get());

    if (wal) {
        auto st = engine.load();
        std::printf("AOF: replayed %zu commands from %s", st.commands, aof.c_str());
        if (st.truncated_bytes) std::printf(" (dropped %zu bytes of torn tail)", st.truncated_bytes);
        std::puts("");
        if (!wal->open()) { std::perror("open aof"); return 1; }
    }

    kv::Server server(engine, host, static_cast<uint16_t>(port), threads);
    g_server = &server;
    if (!server.start()) { std::perror("start"); return 1; }
    std::printf("MaxKV listening on %s:%u with %u worker threads\n", host.c_str(), server.port(), threads);

    // Housekeeping: active expiry + fsync once per second.
    std::atomic<bool> hk_stop{false};
    std::thread hk([&] {
        int tick = 0;
        while (!hk_stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (++tick % 10 == 0) {
                store.sweep_expired();
                if (wal) wal->fsync_now();
            }
        }
    });

    server.run();
    std::puts("\nShutting down...");
    hk_stop.store(true);
    hk.join();
    if (wal) wal->fsync_now();
    return 0;
}
