#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netdb.h>

TEST(NetworkFunc, ResolveHostName) {
  struct sockaddr_in sa; /* input */
  socklen_t len;         /* input */
  char hbuf[NI_MAXHOST];

  memset(&sa, 0, sizeof(struct sockaddr_in));

  /* For IPv4*/
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = inet_addr("172.16.1.11");
  len = sizeof(struct sockaddr_in);

  if (getnameinfo((struct sockaddr *)&sa, len, hbuf, sizeof(hbuf), NULL, 0,
                  NI_NAMEREQD)) {
    printf("could not resolve hostname\n");
  } else {
    printf("host=%s\n", hbuf);
  }
}
