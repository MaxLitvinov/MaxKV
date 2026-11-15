#include "kv/server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

#include "kv/resp.hpp"

namespace kv {

struct Server::Conn {
    int fd;
    std::string in;
    std::string out;
    bool want_close = false;
    bool epollout = false;
};

struct Server::Worker {
    int epfd = -1;
    std::thread th;
    std::mutex mu;
    std::unordered_set<Conn*> conns;
};

namespace {
constexpr size_t kMaxInputBuffer = 128u * 1024 * 1024;
}

Server::Server(Engine& engine, std::string host, uint16_t port, size_t workers)
    : engine_(engine), host_(std::move(host)), port_(port), n_workers_(workers ? workers : 1) {}

Server::~Server() {
    stop_.store(true);
    for (auto& w : workers_)
        if (w->th.joinable()) w->th.join();
    for (auto& w : workers_)
        if (w->epfd >= 0) ::close(w->epfd);
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

bool Server::start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) return false;
    int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) return false;
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    if (::listen(listen_fd_, 511) != 0) return false;
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);

    for (size_t i = 0; i < n_workers_; ++i) {
        auto w = std::make_unique<Worker>();
        w->epfd = ::epoll_create1(EPOLL_CLOEXEC);
        if (w->epfd < 0) return false;
        workers_.push_back(std::move(w));
    }
    for (auto& w : workers_) {
        Worker* wp = w.get();
        wp->th = std::thread([this, wp] { worker_loop(*wp); });
    }
    return true;
}

void Server::run() {
    pollfd p{listen_fd_, POLLIN, 0};
    size_t next = 0;
    while (!stop_.load()) {
        if (::poll(&p, 1, 200) <= 0) continue;
        for (;;) {
            int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) break;
            int one = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            Worker& w = *workers_[next++ % workers_.size()];
            auto* c = new Conn{fd, {}, {}, false, false};
            {
                std::lock_guard lk(w.mu);
                w.conns.insert(c);
            }
            epoll_event ev{};
            ev.events = EPOLLIN;
            ev.data.ptr = c;
            if (::epoll_ctl(w.epfd, EPOLL_CTL_ADD, fd, &ev) != 0) close_conn(w, c);
        }
    }
}

void Server::worker_loop(Worker& w) {
    epoll_event evs[128];
    while (!stop_.load()) {
        int n = ::epoll_wait(w.epfd, evs, 128, 200);
        for (int i = 0; i < n; ++i) {
            auto* c = static_cast<Conn*>(evs[i].data.ptr);
            const uint32_t e = evs[i].events;
            if (e & (EPOLLERR | EPOLLHUP)) {
                close_conn(w, c);
                continue;
            }
            if (e & EPOLLIN) {
                on_readable(w, c);
                continue;  // on_readable flushes and may close
            }
            if (e & EPOLLOUT) {
                if (!flush(w, c)) close_conn(w, c);
                else if (c->want_close && c->out.empty()) close_conn(w, c);
            }
        }
    }
    std::lock_guard lk(w.mu);
    for (Conn* c : w.conns) {
        ::close(c->fd);
        delete c;
    }
    w.conns.clear();
}

void Server::on_readable(Worker& w, Conn* c) {
    char buf[16384];
    for (;;) {
        ssize_t n = ::recv(c->fd, buf, sizeof(buf), 0);
        if (n > 0) {
            c->in.append(buf, static_cast<size_t>(n));
            if (c->in.size() > kMaxInputBuffer) return close_conn(w, c);
            continue;
        }
        if (n == 0) return close_conn(w, c);
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        return close_conn(w, c);
    }

    size_t off = 0;
    while (!c->want_close && off < c->in.size()) {
        ParseResult r = parse_command(std::string_view(c->in).substr(off));
        if (r.status == ParseStatus::Incomplete) break;
        if (r.status == ParseStatus::Error) {
            c->out += reply::error("ERR " + r.error);
            c->want_close = true;
            break;
        }
        off += r.consumed;
        if (r.args.empty()) continue;
        if (to_upper(r.args[0]) == "QUIT") {
            c->out += reply::simple("OK");
            c->want_close = true;
            break;
        }
        c->out += engine_.execute(r.args);
    }
    c->in.erase(0, off);

    if (!flush(w, c) || (c->want_close && c->out.empty())) close_conn(w, c);
}

// Returns false on fatal socket error.
bool Server::flush(Worker& w, Conn* c) {
    size_t sent = 0;
    while (sent < c->out.size()) {
        ssize_t n = ::send(c->fd, c->out.data() + sent, c->out.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<size_t>(n);
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            return false;
        }
    }
    c->out.erase(0, sent);
    const bool need_out = !c->out.empty();
    if (need_out != c->epollout) {
        epoll_event ev{};
        ev.events = EPOLLIN | (need_out ? static_cast<uint32_t>(EPOLLOUT) : 0u);
        ev.data.ptr = c;
        ::epoll_ctl(w.epfd, EPOLL_CTL_MOD, c->fd, &ev);
        c->epollout = need_out;
    }
    return true;
}

void Server::close_conn(Worker& w, Conn* c) {
    ::epoll_ctl(w.epfd, EPOLL_CTL_DEL, c->fd, nullptr);
    ::close(c->fd);
    {
        std::lock_guard lk(w.mu);
        w.conns.erase(c);
    }
    delete c;
}

}  // namespace kv
