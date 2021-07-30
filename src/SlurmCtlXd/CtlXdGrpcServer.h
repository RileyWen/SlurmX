#pragma once

#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <absl/time/internal/test_util.h>
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

  grpc::Status LoadJobs(grpc::ServerContext *context,
                        const SlurmxGrpc::SqueueXRequest *request,
                        SlurmxGrpc::SqueueXReply *response) override;

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
  void XdNodeIsUpCb_(XdNodeId node_id, void *node_data);
  void XdNodeIsDownCb_(XdNodeId node_id, void *);

  /**
   * Actual Handler of AllocateResource RPC.
   * @return kOK if allocation succeeds. kNoResource if the resource is not
   * enough in selected partition. kNonExistent if partition doesn't exist.
   */
  SlurmxErr AllocateResource(const std::string &partition_name,
                             const resource_t &res,
                             SlurmxGrpc::ResourceInfo *res_info);

  SlurmxErr DeallocateResource(XdNodeId node_id, const uuid &resource_uuid);

  void HeartBeatFromNode(const uuid &node_uuid);

  SlurmxErr GetJobsInfo(JobInfoMsg *job_info, const std::string &update_time,
                        uint16_t show_flags);

  SlurmxErr GetJobInfoByJobId(JobInfoMsg *job_info, uint32_t job_id,
                              uint16_t show_flags);

  SlurmxErr GetJobInfoByUserId(JobInfoMsg *job_info, uint32_t user_id,
                               uint16_t show_flags);

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