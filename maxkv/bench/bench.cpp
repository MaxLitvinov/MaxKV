// Load generator: N client threads, optional pipelining, reports throughput and latency percentiles.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

static std::string cmd(std::initializer_list<std::string> a) {
    std::string o = "*" + std::to_string(a.size()) + "\r\n";
    for (auto& s : a) o += "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
    return o;
}

int main(int argc, char** argv) {
    int port = 6380, clients = 16, pipeline = 1;
    long requests = 100000;
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i];
        if (a == "--port") port = atoi(argv[i + 1]);
        else if (a == "--clients") clients = atoi(argv[i + 1]);
        else if (a == "--requests") requests = atol(argv[i + 1]);
        else if (a == "--pipeline") pipeline = atoi(argv[i + 1]);
    }
    const long per_client = requests / clients;
    std::vector<std::vector<double>> lat(clients);
    std::vector<std::thread> ths;
    auto t0 = std::chrono::steady_clock::now();

    for (int c = 0; c < clients; ++c)
        ths.emplace_back([&, c] {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
            inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
            if (connect(fd, (sockaddr*)&a, sizeof(a)) != 0) { std::perror("connect"); return; }
            int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            std::string value(64, 'x');
            char buf[1 << 16];
            for (long done = 0; done < per_client; done += pipeline) {
                std::string batch; int sets = 0;
                for (int p = 0; p < pipeline; ++p) {
                    std::string key = "key:" + std::to_string((done + p) % 10000);
                    if ((done + p) % 10 < 3) { batch += cmd({"SET", key, value}); ++sets; }  // 30% writes
                    else batch += cmd({"GET", key});
                }
                auto s = std::chrono::steady_clock::now();
                send(fd, batch.data(), batch.size(), 0);
                // count replies by CRLF-terminated top-level frames (simple/bulk/null)
                int got = 0; std::string acc;
                while (got < pipeline) {
                    ssize_t r = recv(fd, buf, sizeof(buf), 0);
                    if (r <= 0) return;
                    acc.append(buf, r);
                    size_t pos = 0;
                    for (;;) {
                        if (pos >= acc.size()) break;
                        size_t e = acc.find("\r\n", pos);
                        if (e == std::string::npos) break;
                        size_t next = e + 2;
                        if (acc[pos] == '$' && acc[pos + 1] != '-') {
                            long len = atol(acc.c_str() + pos + 1);
                            if (next + len + 2 > acc.size()) break;
                            next += len + 2;
                        }
                        pos = next; ++got;
                    }
                    acc.erase(0, pos);
                }
                double us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - s).count();
                lat[c].push_back(us);
            }
            close(fd);
        });
    for (auto& t : ths) t.join();
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    std::vector<double> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) { return all.empty() ? 0.0 : all[std::min(all.size() - 1, (size_t)(p * all.size()))]; };
    long total = per_client * clients;
    std::printf("clients=%d pipeline=%d requests=%ld\n", clients, pipeline, total);
    std::printf("throughput: %.0f ops/sec (%.2fs)\n", total / secs, secs);
    std::printf("batch latency  p50=%.0fus  p99=%.0fus  max=%.0fus\n", pct(0.5), pct(0.99), all.empty() ? 0 : all.back());
}
