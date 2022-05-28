#include <grpc++/grpc++.h>
#include <security/_pam_macros.h>
#include <security/pam_ext.h>
#include <security/pam_modules.h>
#include <spdlog/fmt/fmt.h>
#include <sys/stat.h>
#include <syslog.h>

#include <boost/algorithm/string.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>

#include "protos/slurmx.grpc.pb.h"
#include "slurmx/PublicHeader.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

bool QueryPortFromSlurmXd(pam_handle_t *pamh, const std::string &xd_address,
                          uint16_t port_to_query, uint32_t *task_id) {
  std::shared_ptr<Channel> channel =
      grpc::CreateChannel(xd_address, grpc::InsecureChannelCredentials());

  // std::unique_ptr will automatically release the dangling stub.
  std::unique_ptr<SlurmxGrpc::SlurmXd::Stub> stub =
      SlurmxGrpc::SlurmXd::NewStub(channel);

  SlurmxGrpc::QueryTaskIdFromPortRequest request;
  SlurmxGrpc::QueryTaskIdFromPortReply reply;
  ClientContext context;
  Status status;

  request.set_port(port_to_query);

  status = stub->QueryTaskIdFromPort(&context, request, &reply);
  if (!status.ok()) {
    pam_syslog(pamh, LOG_ERR, "QueryTaskIdFromPort gRPC call failed.");
    return false;
  }

  if (reply.ok()) {
    pam_syslog(pamh, LOG_ERR,
               "ssh client with remote port %u belongs to task #%u",
               port_to_query, reply.task_id());
    return true;
  } else {
    pam_syslog(pamh, LOG_ERR,
               "ssh client with remote port %u doesn't belong to any task",
               port_to_query);
    return false;
  }
}

void PamSendMsgToClient(pam_handle_t *pamh, const char *mesg) {
  int rc;
  struct pam_conv *conv;
  void *dummy; /* needed to eliminate warning
                * dereferencing type-punned pointer will
                * break strict-aliasing rules */
  struct pam_message msg[1];
  const struct pam_message *pmsg[1];
  struct pam_response *prsp;

  // Get conversation function to talk with app.
  rc = pam_get_item(pamh, PAM_CONV, (const void **)&dummy);
  conv = (struct pam_conv *)dummy;
  if (rc != PAM_SUCCESS) {
    pam_syslog(pamh, LOG_ERR, "unable to get pam_conv: %s",
               pam_strerror(pamh, rc));
    return;
  }

  // Construct msg to send to app.
  msg[0].msg_style = PAM_ERROR_MSG;
  msg[0].msg = mesg;
  pmsg[0] = &msg[0];
  prsp = nullptr;

  /*  Send msg to app and free the (meaningless) rsp.
   */
  rc = conv->conv(1, pmsg, &prsp, conv->appdata_ptr);
  if (rc != PAM_SUCCESS)
    pam_syslog(pamh, LOG_ERR, "unable to converse with app: %s",
               pam_strerror(pamh, rc));
  if (prsp != nullptr) _pam_drop_reply(prsp, 1);
}

extern "C" {
int pam_sm_acct_mgmt(pam_handle_t *pamh, int flags, int argc,
                     const char **argv) {
  int rc;
  char *user_name = nullptr;

  /* Asking the application for a username */
  rc = pam_get_item(pamh, PAM_USER, (const void **)&user_name);
  if (user_name == nullptr || rc != PAM_SUCCESS) {
    pam_syslog(pamh, LOG_ERR, "[SlurmX] No username in PAM_USER? Fail!");
    return PAM_SESSION_ERR;
  } else {
    pam_syslog(pamh, LOG_ERR, "[SlurmX] Checking user: %s", user_name);
  }

  std::ifstream tcp_file("/proc/net/tcp");
  std::string line;
  if (!tcp_file) {
    pam_syslog(pamh, LOG_ERR, "[SlurmX] Failed to open /proc/net/tcp");
    return PAM_PERM_DENIED;
  }

  std::unordered_map<ino_t, std::string> inode_addr_port_map;

  std::getline(tcp_file, line);
  while (std::getline(tcp_file, line)) {
    boost::trim(line);
    std::vector<std::string> line_vec;
    boost::split(line_vec, line, boost::is_any_of(" "),
                 boost::token_compress_on);

    // 2nd row is remote address and 9th row is inode num.
    pam_syslog(pamh, LOG_ERR, "[SlurmX] TCP conn %s %s, inode: %s",
               line_vec[0].c_str(), line_vec[2].c_str(), line_vec[9].c_str());
    ino_t inode_num = std::stoul(line_vec[9]);
    inode_addr_port_map.emplace(inode_num, line_vec[2]);
  }

#ifndef NDEBUG
  std::string output;
  for (auto &&[k, v] : inode_addr_port_map) {
    output += fmt::format("{}:{} ", k, v);
  }

  pam_syslog(pamh, LOG_ERR, "[SlurmX] inode_addr_port_map: %s", output.c_str());
#endif

  uint8_t addr[4];
  uint16_t port;
  bool src_found{false};

  std::string fds_path = "/proc/self/fd";
  for (const auto &entry : std::filesystem::directory_iterator(fds_path)) {
    // entry must have call stat() once in its implementation.
    // So entry.is_socket() points to the real file.
    if (entry.is_socket()) {
      pam_syslog(pamh, LOG_ERR, "[SlurmX] Checking socket fd %s",
                 entry.path().c_str());
      struct stat stat_buf {};
      // stat() will resolve symbol link.
      if (stat(entry.path().c_str(), &stat_buf) != 0) {
        pam_syslog(pamh, LOG_ERR, "[SlurmX] stat failed for socket fd %s",
                   entry.path().c_str());
        continue;
      } else {
        pam_syslog(pamh, LOG_ERR, "[SlurmX] inode num for socket fd %s is %lu",
                   entry.path().c_str(), stat_buf.st_ino);
      }

      auto iter = inode_addr_port_map.find(stat_buf.st_ino);
      if (iter == inode_addr_port_map.end()) {
        pam_syslog(pamh, LOG_ERR,
                   "[SlurmX] inode num %lu not found in /proc/net/tcp",
                   stat_buf.st_ino);
      } else {
        std::vector<std::string> addr_port_hex;
        boost::split(addr_port_hex, iter->second, boost::is_any_of(":"));

        const std::string &addr_hex = addr_port_hex[0];
        const std::string &port_hex = addr_port_hex[1];
        pam_syslog(pamh, LOG_ERR,
                   "[SlurmX] hex addr and port for inode num %lu: %s:%s",
                   stat_buf.st_ino, addr_hex.c_str(), port_hex.c_str());

        for (int i = 0; i < 4; i++) {
          std::string addr_part = addr_hex.substr(6 - 2 * i, 2);
          addr[i] = std::stoul(addr_part, nullptr, 16);
          pam_syslog(pamh, LOG_ERR,
                     "[SlurmX] Transform %d part of hex addr: %s to int %hhu",
                     i, addr_part.c_str(), addr[i]);
        }

        port = std::stoul(port_hex, nullptr, 16);

        pam_syslog(pamh, LOG_ERR,
                   "[SlurmX] inode num %lu found in /proc/net/tcp: "
                   "%hhu.%hhu.%hhu.%hhu:%hu",
                   stat_buf.st_ino, addr[0], addr[1], addr[2], addr[3], port);

        src_found = true;
        break;
      }
    }
  }

  if (!src_found) {
    PamSendMsgToClient(
        pamh, "SlurmX: Cannot resolve src address and port in pam module.");
    return PAM_PERM_DENIED;
  }

  uint32_t task_id;
  bool belongs_to_a_task{false};
  std::string server_address =
      fmt::format("{}.{}.{}.{}:{}", addr[0], addr[1], addr[2], addr[3], port);

  belongs_to_a_task =
      QueryPortFromSlurmXd(pamh, server_address, port, &task_id);

  if (belongs_to_a_task) {
    pam_syslog(pamh, LOG_ERR,
               "[SlurmX] Accepted ssh connection with remote port %hu ", port);
    return PAM_SUCCESS;
  } else {
    pam_syslog(pamh, LOG_ERR,
               "[SlurmX] Rejected ssh connection with remote port %hu ", port);
    PamSendMsgToClient(pamh, "Rejected by SlurmX");
    return PAM_PERM_DENIED;
  }
}
}