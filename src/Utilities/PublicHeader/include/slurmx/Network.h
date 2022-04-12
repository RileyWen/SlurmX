#pragma once

#include <arpa/inet.h>
#include <netdb.h>

#include <string>

namespace slurmx {

bool ResolveHostnameFromIpv4(const std::string& addr, std::string* hostname);

bool ResolveHostnameFromIpv6(const std::string& addr, std::string* hostname);

}  // namespace slurmx