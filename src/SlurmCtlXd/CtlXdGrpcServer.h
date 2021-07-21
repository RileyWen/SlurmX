#pragma once

#include <grpc++/grpc++.h>

#include <boost/algorithm/string.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

#include "CtlXdPublicDefs.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "slurmx/Lock.h"

namespace CtlXd {

using boost::uuids::uuid;
using grpc::Channel;
using grpc::Server;
using SlurmxGrpc::SlurmXd;

class CtlXdServer;

class SlurmCtlXdServiceImpl final : public SlurmxGrpc::SlurmCtlXd::Service {
 public:
  SlurmCtlXdServiceImpl(CtlXdServer *server) : m_ctlxd_server_(server) {}

  grpc::Status RegisterSlurmXd(
      grpc::ServerContext *context,
      const SlurmxGrpc::SlurmXdRegisterRequest *request,
      SlurmxGrpc::SlurmXdRegisterResult *response) override;

  grpc::Status AllocateResource(
      grpc::ServerContext *context,
      const SlurmxGrpc::ResourceAllocRequest *request,
      SlurmxGrpc::ResourceAllocReply *response) override;

  grpc::Status DeallocateResource(
      grpc::ServerContext *context,
      const SlurmxGrpc::DeallocateResourceRequest *request,
      SlurmxGrpc::DeallocateResourceReply *response) override;

  grpc::Status Heartbeat(grpc::ServerContext *context,
                         const SlurmxGrpc::HeartbeatRequest *request,
                         SlurmxGrpc::HeartbeatReply *response) override;

 private:
  CtlXdServer *m_ctlxd_server_;
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
  void XdNodeIsUpCb_(uint32_t index, void *node_data);
  void XdNodeIsDownCb_(uint32_t index, void *node_data);

  SlurmxErr AllocateResource(const resource_t &res,
                             SlurmxGrpc::ResourceInfo *res_info);
  SlurmxErr DeallocateResource(uint32_t node_index, const uuid &resource_uuid);

  void HeartBeatFromNode(const uuid &node_uuid);

  const std::string m_listen_address_;

  std::unique_ptr<SlurmCtlXdServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  boost::uuids::random_generator_mt19937 m_uuid_gen_;

  inline static std::mutex s_sigint_mtx;
  inline static std::condition_variable s_sigint_cv;
  static void signal_handler_func(int) { s_sigint_cv.notify_one(); };

  friend class SlurmCtlXdServiceImpl;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::CtlXdServer> g_ctlxd_server;