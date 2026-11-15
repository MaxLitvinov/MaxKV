#pragma once
#include <array>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv {

// Wall-clock time in milliseconds (absolute timestamps are persisted in the AOF).
int64_t now_ms();

// Redis-style glob matching: '*' and '?' are supported.
bool glob_match(const std::string& pattern, const std::string& text);

struct Entry {
    std::string value;
    int64_t expire_at = 0;  // absolute ms since epoch, 0 = no expiry
};

struct SnapshotItem {
    std::string key;
    std::string value;
    int64_t expire_at;
};

enum class SetMode { Always, NX, XX };
enum class IncrStatus { Ok, NotInteger, Overflow };

// Thread-safe sharded hash map with lazy + active key expiration.
class Store {
public:
    bool set(const std::string& key, std::string value, int64_t expire_at, SetMode mode);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    bool exists(const std::string& key);
    bool expire_at(const std::string& key, int64_t at_ms);
    // -2: key missing, -1: no expiry, otherwise remaining milliseconds
    int64_t ttl_ms(const std::string& key);
    IncrStatus incr_by(const std::string& key, int64_t delta, int64_t& out);
    std::vector<std::string> keys(const std::string& pattern);
    size_t size();
    void clear();
    size_t sweep_expired();
    std::vector<SnapshotItem> snapshot();

private:
    static constexpr size_t kShards = 16;
    struct Shard {
        std::mutex mu;
        std::unordered_map<std::string, Entry> map;
    };
    std::array<Shard, kShards> shards_;
    Shard& shard_for(const std::string& key);
};

}  // namespace kv
