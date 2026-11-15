#include "kv/engine.hpp"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <sstream>

#include "kv/resp.hpp"

namespace kv {

std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
}

namespace {
bool parse_i64(const std::string& s, int64_t& out) {
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}
const char* kSyntax = "ERR syntax error";
const char* kNotInt = "ERR value is not an integer or out of range";
}  // namespace

Engine::Engine(Store& store, Wal* wal) : store_(store), wal_(wal), started_ms_(now_ms()) {
    register_commands();
}

ReplayStats Engine::load() {
    if (!wal_) return {};
    return wal_->replay([this](const Args& a) { execute(a, /*replaying=*/true); });
}

std::string Engine::execute(const Args& args, bool replaying) {
    if (args.empty()) return reply::error("ERR empty command");
    const std::string name = to_upper(args[0]);
    auto it = table_.find(name);
    if (it == table_.end()) return reply::error("ERR unknown command '" + args[0] + "'");
    const Cmd& cmd = it->second;
    const int n = static_cast<int>(args.size());
    if (n < cmd.min_args || (cmd.max_args >= 0 && n > cmd.max_args))
        return reply::error("ERR wrong number of arguments for '" + name + "' command");

    commands_.fetch_add(1, std::memory_order_relaxed);
    Ctx ctx;
    if (!cmd.write) return cmd.fn(ctx, args);

    std::lock_guard lk(write_mu_);
    std::string out = cmd.fn(ctx, args);
    if (!replaying && wal_)
        for (const auto& l : ctx.log) wal_->append(l);
    return out;
}

void Engine::register_commands() {
    auto add = [this](const char* name, int min, int max, bool write,
                      std::function<std::string(Ctx&, const Args&)> fn) {
        table_[name] = Cmd{min, max, write, std::move(fn)};
    };

    add("PING", 1, 2, false, [](Ctx&, const Args& a) {
        return a.size() > 1 ? reply::bulk(a[1]) : reply::simple("PONG");
    });
    add("ECHO", 2, 2, false, [](Ctx&, const Args& a) { return reply::bulk(a[1]); });
    add("COMMAND", 1, -1, false, [](Ctx&, const Args&) { return reply::empty_array(); });

    add("SET", 3, -1, true, [this](Ctx& c, const Args& a) {
        SetMode mode = SetMode::Always;
        int64_t expire_at = 0;
        bool has_exp = false;
        for (size_t i = 3; i < a.size(); ++i) {
            const std::string opt = to_upper(a[i]);
            if (opt == "NX" || opt == "XX") {
                if (mode != SetMode::Always) return reply::error(kSyntax);
                mode = opt == "NX" ? SetMode::NX : SetMode::XX;
            } else if (opt == "EX" || opt == "PX" || opt == "PXAT") {
                if (has_exp || i + 1 >= a.size()) return reply::error(kSyntax);
                int64_t v;
                if (!parse_i64(a[++i], v) || v <= 0) return reply::error("ERR invalid expire time in 'set' command");
                expire_at = opt == "EX" ? now_ms() + v * 1000 : opt == "PX" ? now_ms() + v : v;
                has_exp = true;
            } else {
                return reply::error(kSyntax);
            }
        }
        if (!store_.set(a[1], a[2], expire_at, mode)) return reply::null_bulk();
        Args l{"SET", a[1], a[2]};
        if (has_exp) {
            l.push_back("PXAT");
            l.push_back(std::to_string(expire_at));
        }
        c.log.push_back(std::move(l));
        return reply::simple("OK");
    });

    add("GET", 2, 2, false, [this](Ctx&, const Args& a) {
        auto v = store_.get(a[1]);
        return v ? reply::bulk(*v) : reply::null_bulk();
    });

    add("MGET", 2, -1, false, [this](Ctx&, const Args& a) {
        std::string out = "*" + std::to_string(a.size() - 1) + "\r\n";
        for (size_t i = 1; i < a.size(); ++i) {
            auto v = store_.get(a[i]);
            out += v ? reply::bulk(*v) : reply::null_bulk();
        }
        return out;
    });

    add("DEL", 2, -1, true, [this](Ctx& c, const Args& a) {
        long long n = 0;
        for (size_t i = 1; i < a.size(); ++i)
            if (store_.del(a[i])) {
                ++n;
                c.log.push_back({"DEL", a[i]});
            }
        return reply::integer(n);
    });

    add("EXISTS", 2, -1, false, [this](Ctx&, const Args& a) {
        long long n = 0;
        for (size_t i = 1; i < a.size(); ++i) n += store_.exists(a[i]) ? 1 : 0;
        return reply::integer(n);
    });

    add("EXPIRE", 3, 3, true, [this](Ctx& c, const Args& a) {
        int64_t s;
        if (!parse_i64(a[2], s)) return reply::error(kNotInt);
        const int64_t at = now_ms() + s * 1000;
        if (!store_.expire_at(a[1], at)) return reply::integer(0);
        c.log.push_back(s <= 0 ? Args{"DEL", a[1]} : Args{"PEXPIREAT", a[1], std::to_string(at)});
        return reply::integer(1);
    });

    add("PEXPIREAT", 3, 3, true, [this](Ctx& c, const Args& a) {
        int64_t at;
        if (!parse_i64(a[2], at)) return reply::error(kNotInt);
        if (!store_.expire_at(a[1], at)) return reply::integer(0);
        c.log.push_back(at <= now_ms() ? Args{"DEL", a[1]} : Args{"PEXPIREAT", a[1], std::to_string(at)});
        return reply::integer(1);
    });

    add("TTL", 2, 2, false, [this](Ctx&, const Args& a) {
        int64_t ms = store_.ttl_ms(a[1]);
        return reply::integer(ms < 0 ? ms : (ms + 999) / 1000);
    });

    auto incr = [this](int64_t delta, const char* log_name) {
        return [this, delta, log_name](Ctx& c, const Args& a) {
            int64_t out;
            switch (store_.incr_by(a[1], delta, out)) {
                case IncrStatus::NotInteger: return reply::error(kNotInt);
                case IncrStatus::Overflow: return reply::error("ERR increment or decrement would overflow");
                case IncrStatus::Ok: break;
            }
            c.log.push_back({log_name, a[1]});
            return reply::integer(out);
        };
    };
    add("INCR", 2, 2, true, incr(1, "INCR"));
    add("DECR", 2, 2, true, incr(-1, "DECR"));

    add("KEYS", 2, 2, false, [this](Ctx&, const Args& a) { return reply::array(store_.keys(a[1])); });
    add("DBSIZE", 1, 1, false, [this](Ctx&, const Args&) { return reply::integer(static_cast<long long>(store_.size())); });

    add("FLUSHALL", 1, 1, true, [this](Ctx& c, const Args&) {
        store_.clear();
        c.log.push_back({"FLUSHALL"});
        return reply::simple("OK");
    });

    add("REWRITE", 1, 1, true, [this](Ctx&, const Args&) {
        if (!wal_) return reply::error("ERR persistence is disabled");
        // write_mu_ is held by execute(), so no write can interleave with the snapshot.
        return wal_->rewrite(store_.snapshot()) ? reply::simple("OK") : reply::error("ERR rewrite failed");
    });

    add("INFO", 1, 1, false, [this](Ctx&, const Args&) { return reply::bulk(info()); });
}

std::string Engine::info() {
    std::ostringstream o;
    o << "# Server\r\n"
      << "maxkv_version:1.0.0\r\n"
      << "uptime_in_seconds:" << (now_ms() - started_ms_) / 1000 << "\r\n"
      << "# Stats\r\n"
      << "total_commands_processed:" << commands_processed() << "\r\n"
      << "# Keyspace\r\n"
      << "keys:" << store_.size() << "\r\n"
      << "# Persistence\r\n"
      << "aof_enabled:" << (wal_ ? 1 : 0) << "\r\n";
    return o.str();
}

}  // namespace kv
