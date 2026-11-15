#include "kv/resp.hpp"

#include <charconv>

namespace kv {
namespace {

constexpr size_t kMaxLine = 64;
constexpr long long kMaxArgs = 1024 * 1024;
constexpr long long kMaxBulk = 64LL * 1024 * 1024;
constexpr size_t kMaxInline = 64 * 1024;

bool parse_ll(std::string_view s, long long& out) {
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

ParseResult fail(const char* msg) {
    ParseResult r;
    r.status = ParseStatus::Error;
    r.error = msg;
    return r;
}

}  // namespace

ParseResult parse_command(std::string_view buf) {
    ParseResult r;
    if (buf.empty()) return r;

    if (buf[0] == '*') {
        size_t pos = 0;
        // returns 1 = line read, 0 = incomplete, -1 = too long
        auto read_line = [&](std::string_view& line) -> int {
            size_t e = buf.find("\r\n", pos);
            if (e == std::string_view::npos) return buf.size() - pos > kMaxLine ? -1 : 0;
            line = buf.substr(pos, e - pos);
            pos = e + 2;
            return 1;
        };
        std::string_view line;
        int rc = read_line(line);
        if (rc == 0) return r;
        if (rc < 0) return fail("Protocol error: line too long");
        long long n;
        if (!parse_ll(line.substr(1), n) || n > kMaxArgs) return fail("Protocol error: invalid multibulk length");
        if (n <= 0) {
            r.status = ParseStatus::Ok;
            r.consumed = pos;
            return r;
        }
        r.args.reserve(static_cast<size_t>(n < 1024 ? n : 1024));
        for (long long i = 0; i < n; ++i) {
            rc = read_line(line);
            if (rc == 0) { r.args.clear(); return r; }
            if (rc < 0) return fail("Protocol error: line too long");
            long long len;
            if (line.empty() || line[0] != '$') return fail("Protocol error: expected '$'");
            if (!parse_ll(line.substr(1), len) || len < 0 || len > kMaxBulk)
                return fail("Protocol error: invalid bulk length");
            if (pos + static_cast<size_t>(len) + 2 > buf.size()) { r.args.clear(); return r; }
            if (buf[pos + len] != '\r' || buf[pos + len + 1] != '\n')
                return fail("Protocol error: bad bulk terminator");
            r.args.emplace_back(buf.substr(pos, static_cast<size_t>(len)));
            pos += static_cast<size_t>(len) + 2;
        }
        r.status = ParseStatus::Ok;
        r.consumed = pos;
        return r;
    }

    // Inline command
    size_t e = buf.find('\n');
    if (e == std::string_view::npos) {
        if (buf.size() > kMaxInline) return fail("Protocol error: inline request too long");
        return r;
    }
    std::string_view line = buf.substr(0, e);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
        size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') ++i;
        if (i > start) r.args.emplace_back(line.substr(start, i - start));
    }
    r.status = ParseStatus::Ok;
    r.consumed = e + 1;
    return r;
}

std::string encode_command(const std::vector<std::string>& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& a : args) {
        out += "$" + std::to_string(a.size()) + "\r\n";
        out += a;
        out += "\r\n";
    }
    return out;
}

namespace reply {
std::string simple(const std::string& s) { return "+" + s + "\r\n"; }
std::string error(const std::string& s) { return "-" + s + "\r\n"; }
std::string integer(long long v) { return ":" + std::to_string(v) + "\r\n"; }
std::string bulk(const std::string& s) { return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n"; }
std::string null_bulk() { return "$-1\r\n"; }
std::string array(const std::vector<std::string>& items) {
    std::string out = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& i : items) out += bulk(i);
    return out;
}
std::string empty_array() { return "*0\r\n"; }
}  // namespace reply

}  // namespace kv
