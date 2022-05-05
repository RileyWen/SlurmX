#pragma once

#include <spdlog/fmt/fmt.h>

#include <boost/algorithm/string.hpp>
#include <list>
#include <regex>
#include <string>
#include <vector>

namespace util {

std::string ReadableMemory(uint64_t memory_bytes);

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *hostlist);

std::string HostNameListToStr(const std::list<std::string> &hostlist);

}  // namespace util