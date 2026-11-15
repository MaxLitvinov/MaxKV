#include "kv/wal.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <fstream>
#include <iterator>

#include "kv/resp.hpp"

namespace kv {
namespace {
bool write_all(int fd, const char* data, size_t len) {
    while (len > 0) {
        ssize_t n = ::write(fd, data, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}
}  // namespace

Wal::Wal(std::string path) : path_(std::move(path)) {}

Wal::~Wal() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
    }
}

ReplayStats Wal::replay(const std::function<void(const std::vector<std::string>&)>& fn) {
    ReplayStats st;
    std::ifstream in(path_, std::ios::binary);
    if (!in) return st;
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();

    size_t off = 0;
    while (off < data.size()) {
        ParseResult r = parse_command(std::string_view(data).substr(off));
        if (r.status != ParseStatus::Ok) break;  // incomplete tail or corruption
        off += r.consumed;
        if (!r.args.empty()) {
            fn(r.args);
            ++st.commands;
        }
    }
    if (off < data.size()) {
        st.truncated_bytes = data.size() - off;
        if (::truncate(path_.c_str(), static_cast<off_t>(off)) != 0)
            std::perror("truncate aof");
    }
    return st;
}

bool Wal::open() {
    std::lock_guard lk(mu_);
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    return fd_ >= 0;
}

bool Wal::append(const std::vector<std::string>& args) {
    const std::string buf = encode_command(args);
    std::lock_guard lk(mu_);
    if (fd_ < 0) return false;
    dirty_ = true;
    return write_all(fd_, buf.data(), buf.size());
}

void Wal::fsync_now() {
    std::lock_guard lk(mu_);
    if (fd_ >= 0 && dirty_) {
        ::fdatasync(fd_);
        dirty_ = false;
    }
}

bool Wal::rewrite(const std::vector<SnapshotItem>& items) {
    std::lock_guard lk(mu_);
    const std::string tmp = path_ + ".rewrite";
    int tfd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (tfd < 0) return false;
    for (const auto& it : items) {
        std::vector<std::string> cmd{"SET", it.key, it.value};
        if (it.expire_at != 0) {
            cmd.push_back("PXAT");
            cmd.push_back(std::to_string(it.expire_at));
        }
        const std::string buf = encode_command(cmd);
        if (!write_all(tfd, buf.data(), buf.size())) {
            ::close(tfd);
            ::unlink(tmp.c_str());
            return false;
        }
    }
    ::fsync(tfd);
    ::close(tfd);
    if (::rename(tmp.c_str(), path_.c_str()) != 0) return false;
    if (fd_ >= 0) ::close(fd_);
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    dirty_ = false;
    return fd_ >= 0;
}

}  // namespace kv
