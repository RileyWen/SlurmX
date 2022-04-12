#include "slurmx/Network.h"

#include <cstring>

#include "slurmx/PublicHeader.h"

namespace slurmx {

bool ResolveHostnameFromIpv4(const std::string& addr, std::string* hostname) {
  struct sockaddr_in sa; /* input */
  socklen_t len;         /* input */
  char hbuf[NI_MAXHOST];

  std::memset(&sa, 0, sizeof(struct sockaddr_in));

  /* For IPv4*/
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = inet_addr(addr.c_str());
  len = sizeof(struct sockaddr_in);

  int r;
  if ((r = getnameinfo((struct sockaddr*)&sa, len, hbuf, sizeof(hbuf), nullptr,
                       0, NI_NAMEREQD))) {
    SLURMX_TRACE("Error in getnameinfo when resolving hostname for {}: {}",
                 addr.c_str(), gai_strerror(r));
    return false;
  } else {
    hostname->assign(hbuf);
    return true;
  }
}

bool ResolveHostnameFromIpv6(const std::string& addr, std::string* hostname) {
  struct sockaddr_in6 sa6; /* input */
  socklen_t len;           /* input */
  char hbuf[NI_MAXHOST];

  std::memset(&sa6, 0, sizeof(struct sockaddr_in6));

  /* For IPv4*/
  sa6.sin6_family = AF_INET6;
  in6_addr addr6{};
  inet_pton(AF_INET6, addr.c_str(), &addr6);
  sa6.sin6_addr = addr6;

  len = sizeof(struct sockaddr_in6);

  int r;
  if ((r = getnameinfo((struct sockaddr*)&sa6, len, hbuf, sizeof(hbuf), nullptr,
                       0, NI_NAMEREQD))) {
    SLURMX_TRACE("Error in getnameinfo when resolving hostname for {}: {}",
                 addr.c_str(), gai_strerror(r));
    return false;
  } else {
    hostname->assign(hbuf);
    return true;
  }
}

}  // namespace slurmx
