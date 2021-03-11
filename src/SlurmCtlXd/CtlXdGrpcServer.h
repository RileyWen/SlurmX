#pragma once

#include <grpc++/grpc++.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_hash.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "ConcurrentResourceMgr.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace CtlXd {

using grpc::Channel;
using grpc::Server;
using slurmx_grpc::SlurmXd;

class CtlXdServer;

class SlurmCtlXdServiceImpl final : public slurmx_grpc::SlurmCtlXd::Service {
 public:
  SlurmCtlXdServiceImpl(CtlXdServer *server) : m_ctlxd_server_(server) {}

  grpc::Status RegisterSlurmXd(
      grpc::ServerContext *context,
      const slurmx_grpc::SlurmXdRegisterRequest *request,
      slurmx_grpc::SlurmXdRegisterResult *response) override;

  grpc::Status AllocateResource(
      grpc::ServerContext *context,
      const slurmx_grpc::ResourceAllocRequest *request,
      slurmx_grpc::ResourceAllocReply *response) override;

  grpc::Status Heartbeat(grpc::ServerContext *context,
                         const slurmx_grpc::HeartbeatRequest *request,
                         slurmx_grpc::HeartbeatReply *response) override;

 private:
  CtlXdServer *m_ctlxd_server_;
};

class XdClient {
 public:
  XdClient() = default;

  /***
   * Connect the CtlXdClient to SlurmCtlXd.
   * @param server_address The "[Address]:[Port]" of SlurmCtlXd.
   * @return
   * If SlurmCtlXd is successfully connected, kOk is returned. <br>
   * If SlurmCtlXd cannot be connected within 3s, kConnectionTimeout is
   * returned.
   */
  SlurmxErr Connect(const std::string &server_address);

 private:
  std::shared_ptr<Channel> m_xd_channel_;

  std::unique_ptr<SlurmXd::Stub> m_stub_;
};

/***
 * Note: There should be only ONE instance of CtlXdServer!!!!
 */
class CtlXdServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   * @param listen_address The "[Address]:[Port]" of SlurmCtlXd.
   */
  CtlXdServer(const std::string &listen_address);

  inline void Wait() { m_server_->Wait(); }

 private:
  /***
   *
   * @param[in] addr_port
   * @param[in] node_res
   * @param[out] xd_node_uuid
   * @return
   */
  SlurmxErr RegisterNewXd(const std::string addr_port,
                          const resource_t &node_res, uuid *xd_node_uuid);

  const std::string m_listen_address_;

  std::unique_ptr<SlurmCtlXdServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  ConcurrentResourceMgr *m_resource_mgr_;

  std::mutex m_xd_maps_mtx_;
  std::unordered_map<uuid, std::unique_ptr<XdClient>> m_xd_maps_;

  boost::uuids::random_generator_mt19937 m_uuid_gen_;

  inline static std::mutex s_sigint_mtx;
  inline static std::condition_variable s_sigint_cv;
  static void signal_handler_func(int) { s_sigint_cv.notify_one(); };

  friend class SlurmCtlXdServiceImpl;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::CtlXdServer> g_ctlxd_server;