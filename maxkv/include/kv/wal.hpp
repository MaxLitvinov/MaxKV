#pragma once
#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "kv/store.hpp"

namespace kv {

struct ReplayStats {
    size_t commands = 0;
    size_t truncated_bytes = 0;  // bytes of a torn/corrupt tail that were dropped
};

// Append-only file: every mutating command is persisted in RESP format.
// On startup the file is replayed; a torn tail (crash mid-write) is detected and cut off.
class Wal {
public:
    explicit Wal(std::string path);
    ~Wal();
    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    ReplayStats replay(const std::function<void(const std::vector<std::string>&)>& fn);
    bool open();
    bool append(const std::vector<std::string>& args);
    void fsync_now();
    // Compacts the log: writes a minimal command set that recreates `items`, then atomically swaps files.
    bool rewrite(const std::vector<SnapshotItem>& items);
    const std::string& path() const { return path_; }

private:
    std::string path_;
    int fd_ = -1;
    bool dirty_ = false;
    std::mutex mu_;
};

}  // namespace kv
