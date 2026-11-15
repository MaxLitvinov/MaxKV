// Dependency-free test runner: unit, persistence, protocol, network and concurrency tests.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "kv/engine.hpp"
#include "kv/resp.hpp"
#include "kv/server.hpp"
#include "kv/store.hpp"
#include "kv/wal.hpp"

using namespace kv;

struct TestCase { const char* name; void (*fn)(); };
static std::vector<TestCase>& registry() { static std::vector<TestCase> r; return r; }
struct Reg { Reg(const char* n, void (*f)()) { registry().push_back({n, f}); } };
static int g_failures = 0;

#define TEST(name) static void name(); static Reg reg_##name(#name, name); static void name()
#define CHECK(cond) do { if (!(cond)) { std::fprintf(stderr, "    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); ++g_failures; } } while (0)
#define CHECK_EQ(a, b) do { auto _a = (a); auto _b = (b); if (!(_a == _b)) { std::fprintf(stderr, "    FAIL %s:%d: %s == %s\n", __FILE__, __LINE__, #a, #b); ++g_failures; } } while (0)

static Args A(std::initializer_list<const char*> l) { return Args(l.begin(), l.end()); }
static std::string tmp_path(const char* tag) {
    return std::string("/tmp/maxkv_test_") + tag + "_" + std::to_string(::getpid()) + ".aof";
}

// ---------------- Store ----------------
TEST(store_set_get_del) {
    Store s;
    CHECK(s.set("a", "1", 0, SetMode::Always));
    CHECK_EQ(*s.get("a"), std::string("1"));
    CHECK(s.del("a"));
    CHECK(!s.del("a"));
    CHECK(!s.get("a").has_value());
}
TEST(store_nx_xx) {
    Store s;
    CHECK(!s.set("k", "v", 0, SetMode::XX));
    CHECK(s.set("k", "v", 0, SetMode::NX));
    CHECK(!s.set("k", "w", 0, SetMode::NX));
    CHECK(s.set("k", "w", 0, SetMode::XX));
    CHECK_EQ(*s.get("k"), std::string("w"));
}
TEST(store_expiry) {
    Store s;
    s.set("k", "v", now_ms() + 50, SetMode::Always);
    CHECK(s.get("k").has_value());
    CHECK(s.ttl_ms("k") > 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    CHECK(!s.get("k").has_value());
    CHECK_EQ(s.ttl_ms("k"), (int64_t)-2);
    s.set("a", "1", now_ms() + 10, SetMode::Always);
    s.set("b", "1", 0, SetMode::Always);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK_EQ(s.sweep_expired(), (size_t)1);
    CHECK_EQ(s.size(), (size_t)1);
}
TEST(store_incr) {
    Store s;
    int64_t v = 0;
    CHECK(s.incr_by("n", 5, v) == IncrStatus::Ok);
    CHECK_EQ(v, (int64_t)5);
    s.set("s", "abc", 0, SetMode::Always);
    CHECK(s.incr_by("s", 1, v) == IncrStatus::NotInteger);
    s.set("big", "9223372036854775807", 0, SetMode::Always);
    CHECK(s.incr_by("big", 1, v) == IncrStatus::Overflow);
}
TEST(glob) {
    CHECK(glob_match("*", "anything"));
    CHECK(glob_match("user:*", "user:42"));
    CHECK(glob_match("h?llo", "hello"));
    CHECK(glob_match("*:*:end", "a:b:end"));
    CHECK(!glob_match("user:*", "usr:42"));
    CHECK(glob_match("*", "*abc"));
    CHECK(!glob_match("a*b", "a"));
}

// ---------------- RESP ----------------
TEST(resp_parse_array) {
    auto r = parse_command("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    CHECK(r.status == ParseStatus::Ok);
    CHECK_EQ(r.args.size(), (size_t)2);
    CHECK_EQ(r.args[0], std::string("GET"));
    CHECK_EQ(r.consumed, (size_t)20);
}
TEST(resp_incomplete_at_every_split) {
    const std::string full = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n";
    for (size_t i = 0; i < full.size(); ++i)
        CHECK(parse_command(std::string_view(full).substr(0, i)).status == ParseStatus::Incomplete);
    CHECK(parse_command(full).status == ParseStatus::Ok);
}
TEST(resp_binary_safe_and_pipelined) {
    std::string two = encode_command({"SET", std::string("a\r\nb", 4), "x"}) + encode_command({"GET", "k"});
    auto r1 = parse_command(two);
    CHECK(r1.status == ParseStatus::Ok);
    CHECK_EQ(r1.args[1].size(), (size_t)4);
    auto r2 = parse_command(std::string_view(two).substr(r1.consumed));
    CHECK_EQ(r2.args[0], std::string("GET"));
}
TEST(resp_inline_and_errors) {
    auto r = parse_command("SET  a   b\r\n");
    CHECK(r.status == ParseStatus::Ok);
    CHECK_EQ(r.args.size(), (size_t)3);
    CHECK(parse_command("*x\r\n").status == ParseStatus::Error);
    CHECK(parse_command("*1\r\n$-5\r\n").status == ParseStatus::Error);
    CHECK(parse_command("*1\r\n$3\r\nabcXY").status == ParseStatus::Error);
}

// ---------------- Engine ----------------
TEST(engine_commands) {
    Store s;
    Engine e(s);
    CHECK_EQ(e.execute(A({"PING"})), std::string("+PONG\r\n"));
    CHECK_EQ(e.execute(A({"set", "k", "v"})), std::string("+OK\r\n"));
    CHECK_EQ(e.execute(A({"GET", "k"})), std::string("$1\r\nv\r\n"));
    CHECK_EQ(e.execute(A({"GET", "nope"})), std::string("$-1\r\n"));
    CHECK_EQ(e.execute(A({"SET", "k", "v", "NX"})), std::string("$-1\r\n"));
    CHECK_EQ(e.execute(A({"INCR", "n"})), std::string(":1\r\n"));
    CHECK_EQ(e.execute(A({"INCR", "n"})), std::string(":2\r\n"));
    CHECK_EQ(e.execute(A({"EXISTS", "k", "n", "zz"})), std::string(":2\r\n"));
    CHECK_EQ(e.execute(A({"MGET", "k", "zz"})), std::string("*2\r\n$1\r\nv\r\n$-1\r\n"));
    CHECK_EQ(e.execute(A({"DEL", "k", "n"})), std::string(":2\r\n"));
    CHECK_EQ(e.execute(A({"DBSIZE"})), std::string(":0\r\n"));
    CHECK_EQ(e.execute(A({"TTL", "nope"})), std::string(":-2\r\n"));
}
TEST(engine_errors) {
    Store s;
    Engine e(s);
    CHECK(e.execute(A({"NOPE"})).rfind("-ERR unknown command", 0) == 0);
    CHECK(e.execute(A({"GET"})).rfind("-ERR wrong number", 0) == 0);
    CHECK(e.execute(A({"SET", "k", "v", "EX", "abc"})).rfind("-ERR", 0) == 0);
    CHECK(e.execute(A({"SET", "k", "v", "NX", "XX"})).rfind("-ERR syntax", 0) == 0);
    e.execute(A({"SET", "s", "text"}));
    CHECK(e.execute(A({"INCR", "s"})).rfind("-ERR value is not an integer", 0) == 0);
}
TEST(engine_ttl_commands) {
    Store s;
    Engine e(s);
    e.execute(A({"SET", "k", "v", "EX", "100"}));
    std::string t = e.execute(A({"TTL", "k"}));
    CHECK(t == ":100\r\n" || t == ":99\r\n");
    CHECK_EQ(e.execute(A({"EXPIRE", "k", "0"})), std::string(":1\r\n"));
    CHECK_EQ(e.execute(A({"GET", "k"})), std::string("$-1\r\n"));
}

// ---------------- Persistence ----------------
TEST(aof_roundtrip_and_recovery) {
    const std::string path = tmp_path("rt");
    ::unlink(path.c_str());
    {
        Store s; Wal w(path); Engine e(s, &w);
        CHECK(w.open());
        e.execute(A({"SET", "a", "1"}));
        e.execute(A({"SET", "b", "2", "EX", "1000"}));
        e.execute(A({"INCR", "n"}));
        e.execute(A({"INCR", "n"}));
        e.execute(A({"SET", "gone", "x"}));
        e.execute(A({"DEL", "gone"}));
        e.execute(A({"EXPIRE", "a", "500"}));
    }
    {
        Store s; Wal w(path); Engine e(s, &w);
        auto st = e.load();
        CHECK_EQ(st.commands, (size_t)7);
        CHECK_EQ(*s.get("a"), std::string("1"));
        CHECK_EQ(*s.get("n"), std::string("2"));
        CHECK(!s.get("gone").has_value());
        CHECK(s.ttl_ms("a") > 400000);
        CHECK(s.ttl_ms("b") > 900000);
    }
    ::unlink(path.c_str());
}
TEST(aof_torn_tail_is_truncated) {
    const std::string path = tmp_path("torn");
    ::unlink(path.c_str());
    {
        Store s; Wal w(path); Engine e(s, &w);
        w.open();
        e.execute(A({"SET", "ok", "1"}));
    }
    FILE* f = std::fopen(path.c_str(), "ab");
    std::fputs("*3\r\n$3\r\nSET\r\n$4\r\ntorn\r\n$10\r\nabc", f);  // crash mid-write
    std::fclose(f);
    Store s; Wal w(path); Engine e(s, &w);
    auto st = e.load();
    CHECK_EQ(st.commands, (size_t)1);
    CHECK(st.truncated_bytes > 0);
    CHECK(s.get("ok").has_value());
    CHECK(!s.get("torn").has_value());
    // after truncation the file is clean again
    Store s2; Wal w2(path); Engine e2(s2, &w2);
    CHECK_EQ(e2.load().truncated_bytes, (size_t)0);
    ::unlink(path.c_str());
}
TEST(aof_rewrite_compacts) {
    const std::string path = tmp_path("rw");
    ::unlink(path.c_str());
    long before = 0, after = 0;
    {
        Store s; Wal w(path); Engine e(s, &w);
        w.open();
        for (int i = 0; i < 500; ++i) e.execute(A({"SET", "hot", std::to_string(i).c_str()}));
        e.execute(A({"SET", "cold", "x"}));
        FILE* f = std::fopen(path.c_str(), "rb"); std::fseek(f, 0, SEEK_END); before = std::ftell(f); std::fclose(f);
        CHECK_EQ(e.execute(A({"REWRITE"})), std::string("+OK\r\n"));
        f = std::fopen(path.c_str(), "rb"); std::fseek(f, 0, SEEK_END); after = std::ftell(f); std::fclose(f);
        e.execute(A({"SET", "post", "y"}));  // appends still work after the swap
    }
    CHECK(after < before / 20);
    Store s; Wal w(path); Engine e(s, &w);
    e.load();
    CHECK_EQ(*s.get("hot"), std::string("499"));
    CHECK(s.get("cold").has_value() && s.get("post").has_value());
    ::unlink(path.c_str());
}

// ---------------- Network ----------------
static int connect_to(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    timeval tv{3, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (::connect(fd, (sockaddr*)&a, sizeof(a)) != 0) { ::close(fd); return -1; }
    return fd;
}
static std::string read_n(int fd, size_t n) {
    std::string out; char buf[4096];
    while (out.size() < n) {
        ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) break;
        out.append(buf, r);
    }
    return out;
}
static void send_all(int fd, const std::string& s) { ::send(fd, s.data(), s.size(), MSG_NOSIGNAL); }

struct TestServer {
    Store store; Engine engine{store}; Server server{engine, "127.0.0.1", 0, 4}; std::thread th;
    TestServer() { if (!server.start()) { std::fprintf(stderr, "server start failed\n"); std::exit(1); } th = std::thread([this] { server.run(); }); }
    ~TestServer() { server.stop(); th.join(); }
};

TEST(net_basic_and_inline) {
    TestServer ts;
    int fd = connect_to(ts.server.port());
    CHECK(fd >= 0);
    send_all(fd, "PING\r\n");
    CHECK_EQ(read_n(fd, 7), std::string("+PONG\r\n"));
    send_all(fd, encode_command({"SET", "k", "v"}));
    CHECK_EQ(read_n(fd, 5), std::string("+OK\r\n"));
    send_all(fd, encode_command({"GET", "k"}));
    CHECK_EQ(read_n(fd, 7), std::string("$1\r\nv\r\n"));
    ::close(fd);
}
TEST(net_pipelining_and_fragmentation) {
    TestServer ts;
    int fd = connect_to(ts.server.port());
    std::string req;
    for (int i = 0; i < 1000; ++i) req += encode_command({"INCR", "c"});
    send_all(fd, req);
    std::string expect;
    for (int i = 1; i <= 1000; ++i) expect += ":" + std::to_string(i) + "\r\n";
    CHECK_EQ(read_n(fd, expect.size()), expect);
    // byte-by-byte delivery
    std::string cmd = encode_command({"SET", "slow", "value"});
    for (char ch : cmd) { send_all(fd, std::string(1, ch)); }
    CHECK_EQ(read_n(fd, 5), std::string("+OK\r\n"));
    ::close(fd);
}
TEST(net_large_value) {
    TestServer ts;
    int fd = connect_to(ts.server.port());
    std::string big(5 * 1024 * 1024, 'x');
    send_all(fd, encode_command({"SET", "big", big}));
    CHECK_EQ(read_n(fd, 5), std::string("+OK\r\n"));
    send_all(fd, encode_command({"GET", "big"}));
    std::string hdr = "$" + std::to_string(big.size()) + "\r\n";
    std::string got = read_n(fd, hdr.size() + big.size() + 2);
    CHECK_EQ(got.size(), hdr.size() + big.size() + 2);
    CHECK(got.compare(0, hdr.size(), hdr) == 0);
    ::close(fd);
}
TEST(net_protocol_error_and_quit) {
    TestServer ts;
    int fd = connect_to(ts.server.port());
    send_all(fd, "*abc\r\n");
    CHECK(read_n(fd, 4).rfind("-ERR", 0) == 0);
    ::close(fd);
    fd = connect_to(ts.server.port());
    send_all(fd, "QUIT\r\n");
    CHECK_EQ(read_n(fd, 5), std::string("+OK\r\n"));
    char c; CHECK_EQ(::recv(fd, &c, 1, 0), (ssize_t)0);  // server closed the connection
    ::close(fd);
}
TEST(net_concurrent_incr_is_atomic) {
    TestServer ts;
    constexpr int kClients = 16, kOps = 2000;
    std::vector<std::thread> ths;
    for (int t = 0; t < kClients; ++t)
        ths.emplace_back([&] {
            int fd = connect_to(ts.server.port());
            std::string req, expect_len;
            for (int i = 0; i < kOps; ++i) req += encode_command({"INCR", "shared"});
            send_all(fd, req);
            size_t total = 0;
            while (total < (size_t)kOps * 4) {  // at least ":N\r\n" per reply
                char buf[65536]; ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
                if (r <= 0) break;
                total += r;
            }
            ::close(fd);
        });
    for (auto& t : ths) t.join();
    CHECK_EQ(*ts.store.get("shared"), std::to_string(kClients * kOps));
}
TEST(engine_concurrent_mixed_ops_no_crash) {
    Store s; Engine e(s);
    std::atomic<bool> stop{false};
    std::vector<std::thread> ths;
    for (int t = 0; t < 8; ++t)
        ths.emplace_back([&, t] {
            for (int i = 0; i < 5000; ++i) {
                std::string k = "k" + std::to_string((i * 7 + t) % 200);
                e.execute(A({"SET", k.c_str(), "v", "PX", "5"}));
                e.execute(A({"GET", k.c_str()}));
                e.execute(A({"DEL", k.c_str()}));
                if (i % 500 == 0) e.execute(A({"KEYS", "k*"}));
            }
        });
    for (auto& t : ths) t.join();
    CHECK(true);
}

int main() {
    int ran = 0;
    for (auto& t : registry()) {
        int before = g_failures;
        std::printf("[ RUN  ] %s\n", t.name);
        t.fn();
        std::printf("[ %s ] %s\n", g_failures == before ? " OK " : "FAIL", t.name);
        ++ran;
    }
    std::printf("\n%d tests, %d failed checks\n", ran, g_failures);
    return g_failures ? 1 : 0;
}
