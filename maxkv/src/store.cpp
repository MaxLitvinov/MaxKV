#include "kv/store.hpp"

#include <charconv>
#include <chrono>
#include <functional>

namespace kv {

int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

bool glob_match(const std::string& p, const std::string& s) {
    size_t pi = 0, si = 0, star = std::string::npos, mark = 0;
    while (si < s.size()) {
        if (pi < p.size() && p[pi] == '*') {
            star = pi++;
            mark = si;
        } else if (pi < p.size() && (p[pi] == '?' || p[pi] == s[si])) {
            ++pi;
            ++si;
        } else if (star != std::string::npos) {
            pi = star + 1;
            si = ++mark;
        } else {
            return false;
        }
    }
    while (pi < p.size() && p[pi] == '*') ++pi;
    return pi == p.size();
}

static bool is_expired(const Entry& e, int64_t now) {
    return e.expire_at != 0 && e.expire_at <= now;
}

Store::Shard& Store::shard_for(const std::string& key) {
    return shards_[std::hash<std::string>{}(key) % kShards];
}

bool Store::set(const std::string& key, std::string value, int64_t expire_at, SetMode mode) {
    auto& sh = shard_for(key);
    std::lock_guard lk(sh.mu);
    const int64_t now = now_ms();
    auto it = sh.map.find(key);
    bool live = it != sh.map.end() && !is_expired(it->second, now);
    if (it != sh.map.end() && !live) {
        sh.map.erase(it);
        it = sh.map.end();
    }
    if (mode == SetMode::NX && live) return false;
    if (mode == SetMode::XX && !live) return false;
    if (expire_at != 0 && expire_at <= now) {  // already expired -> behaves like DEL
        sh.map.erase(key);
        return true;
    }
    sh.map[key] = Entry{std::move(value), expire_at};
    return true;
}

std::optional<std::string> Store::get(const std::string& key) {
    auto& sh = shard_for(key);
    std::lock_guard lk(sh.mu);
    auto it = sh.map.find(key);
    if (it == sh.map.end()) return std::nullopt;
    if (is_expired(it->second, now_ms())) {
        sh.map.erase(it);
        return std::nullopt;
    }
    return it->second.value;
}

bool Store::del(const std::string& key) {
    auto& sh = shard_for(key);
    std::lock_guard lk(sh.mu);
    auto it = sh.map.find(key);
    if (it == sh.map.end()) return false;
    const bool live = !is_expired(it->second, now_ms());
    sh.map.erase(it);
    return live;
}

bool Store::exists(const std::string& key) { return get(key).has_value(); }

bool Store::expire_at(const std::string& key, int64_t at_ms) {
    auto& sh = shard_for(key);
    std::lock_guard lk(sh.mu);
    const int64_t now = now_ms();
    auto it = sh.map.find(key);
    if (it == sh.map.end()) return false;
    if (is_expired(it->second, now) || at_ms <= now) {
        const bool live = !is_expired(it->second, now);
        sh.map.erase(it);
        return live;
    }
    it->second.expire_at = at_ms;
    return true;
}

int64_t Store::ttl_ms(const std::string& key) {
    auto& sh = shard_for(key);
    std::lock_guard lk(sh.mu);
    const int64_t now = now_ms();
    auto it = sh.map.find(key);
    if (it == sh.map.end()) return -2;
    if (is_expired(it->second, now)) {
        sh.map.erase(it);
        return -2;
    }
    return it->second.expire_at == 0 ? -1 : it->second.expire_at - now;
}

IncrStatus Store::incr_by(const std::string& key, int64_t delta, int64_t& out) {
    auto& sh = shard_for(key);
    std::lock_guard lk(sh.mu);
    auto it = sh.map.find(key);
    if (it != sh.map.end() && is_expired(it->second, now_ms())) {
        sh.map.erase(it);
        it = sh.map.end();
    }
    int64_t cur = 0;
    if (it != sh.map.end()) {
        const std::string& v = it->second.value;
        auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), cur);
        if (ec != std::errc() || p != v.data() + v.size()) return IncrStatus::NotInteger;
    }
    int64_t nv;
    if (__builtin_add_overflow(cur, delta, &nv)) return IncrStatus::Overflow;
    if (it != sh.map.end())
        it->second.value = std::to_string(nv);
    else
        sh.map[key] = Entry{std::to_string(nv), 0};
    out = nv;
    return IncrStatus::Ok;
}

std::vector<std::string> Store::keys(const std::string& pattern) {
    std::vector<std::string> out;
    const int64_t now = now_ms();
    for (auto& sh : shards_) {
        std::lock_guard lk(sh.mu);
        for (auto& [k, e] : sh.map)
            if (!is_expired(e, now) && glob_match(pattern, k)) out.push_back(k);
    }
    return out;
}

size_t Store::size() {
    size_t n = 0;
    const int64_t now = now_ms();
    for (auto& sh : shards_) {
        std::lock_guard lk(sh.mu);
        for (auto& [k, e] : sh.map)
            if (!is_expired(e, now)) ++n;
    }
    return n;
}

void Store::clear() {
    for (auto& sh : shards_) {
        std::lock_guard lk(sh.mu);
        sh.map.clear();
    }
}

size_t Store::sweep_expired() {
    size_t removed = 0;
    const int64_t now = now_ms();
    for (auto& sh : shards_) {
        std::lock_guard lk(sh.mu);
        for (auto it = sh.map.begin(); it != sh.map.end();) {
            if (is_expired(it->second, now)) {
                it = sh.map.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
    }
    return removed;
}

std::vector<SnapshotItem> Store::snapshot() {
    std::vector<SnapshotItem> out;
    const int64_t now = now_ms();
    for (auto& sh : shards_) {
        std::lock_guard lk(sh.mu);
        for (auto& [k, e] : sh.map)
            if (!is_expired(e, now)) out.push_back({k, e.value, e.expire_at});
    }
    return out;
}

}  // namespace kv
