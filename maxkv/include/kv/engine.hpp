#pragma once
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv/store.hpp"
#include "kv/wal.hpp"

namespace kv {

using Args = std::vector<std::string>;

// Executes commands against the Store and (optionally) logs mutations to the AOF.
// Reads run fully in parallel; writes are serialized so AOF order == apply order.
class Engine {
public:
    explicit Engine(Store& store, Wal* wal = nullptr);

    // Returns a complete RESP reply.
    std::string execute(const Args& args, bool replaying = false);

    // Replays the AOF into the store. Returns stats.
    ReplayStats load();

    uint64_t commands_processed() const { return commands_.load(std::memory_order_relaxed); }

private:
    struct Ctx {
        std::vector<Args> log;  // normalized commands to persist
    };
    struct Cmd {
        int min_args;
        int max_args;  // -1 = unlimited
        bool write;
        std::function<std::string(Ctx&, const Args&)> fn;
    };

    void register_commands();
    std::string info();

    Store& store_;
    Wal* wal_;
    std::mutex write_mu_;
    std::unordered_map<std::string, Cmd> table_;
    std::atomic<uint64_t> commands_{0};
    int64_t started_ms_;
};

std::string to_upper(std::string s);

}  // namespace kv
