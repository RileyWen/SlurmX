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

}  // namespace slurmx
