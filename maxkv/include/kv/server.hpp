#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "kv/engine.hpp"

namespace kv {

// Multi-reactor TCP server: one acceptor + N worker threads, each running its
// own epoll loop over non-blocking sockets. Pipelined requests are supported.
class Server {
public:
    Server(Engine& engine, std::string host, uint16_t port, size_t workers);
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start();                 // bind + listen + spawn workers
    void run();                   // accept loop; returns after stop()
    void stop() { stop_.store(true); }  // async-signal-safe
    uint16_t port() const { return port_; }

private:
    struct Worker;
    struct Conn;

    void worker_loop(Worker& w);
    void on_readable(Worker& w, Conn* c);
    bool flush(Worker& w, Conn* c);
    void close_conn(Worker& w, Conn* c);

    Engine& engine_;
    std::string host_;
    uint16_t port_;
    size_t n_workers_;
    int listen_fd_ = -1;
    std::atomic<bool> stop_{false};
    std::vector<std::unique_ptr<Worker>> workers_;
};

}  // namespace kv
