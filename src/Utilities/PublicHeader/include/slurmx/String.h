#pragma once

#include <string>
#include <list>
#include <regex>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <spdlog/fmt/fmt.h>
#include <sstream>
#include <iomanip>

namespace util {

std::string ReadableMemory(uint64_t memory_bytes);

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *hostlist);

std::string HostNameListToStr(const std::list<std::string> &hostlist);

}  // namespace slurmx