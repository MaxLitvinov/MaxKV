#pragma once
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

enum class ParseStatus { Ok, Incomplete, Error };

struct ParseResult {
    ParseStatus status = ParseStatus::Incomplete;
    size_t consumed = 0;
    std::vector<std::string> args;
    std::string error;
};

// Parses one command from the front of `buf`. Supports RESP arrays of bulk
// strings (what redis-cli and client libraries send) and inline commands
// (what you type into telnet / nc).
ParseResult parse_command(std::string_view buf);

// Encodes a command as a RESP array (used by the append-only file).
std::string encode_command(const std::vector<std::string>& args);

namespace reply {
std::string simple(const std::string& s);
std::string error(const std::string& s);
std::string integer(long long v);
std::string bulk(const std::string& s);
std::string null_bulk();
std::string array(const std::vector<std::string>& items);  // array of bulk strings
std::string empty_array();
}  // namespace reply

}  // namespace kv
