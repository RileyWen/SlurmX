#pragma once

#include <absl/container/node_hash_map.h>
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
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "slurmx/Lock.h"
#include "slurmx/PublicHeader.h"

namespace CtlXd {

using boost::uuids::uuid;
using grpc::Channel;
using grpc::Server;
using SlurmxGrpc::SlurmXd;

class CtlXdServer;

class SlurmCtlXdServiceImpl final : public SlurmxGrpc::SlurmCtlXd::Service {
 public:
  explicit SlurmCtlXdServiceImpl(CtlXdServer *server)
      : m_ctlxd_server_(server) {}

  grpc::Status AllocateInteractiveTask(
      grpc::ServerContext *context,
      const SlurmxGrpc::InteractiveTaskAllocRequest *request,
      SlurmxGrpc::InteractiveTaskAllocReply *response) override;

  grpc::Status QueryInteractiveTaskAllocDetail(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryInteractiveTaskAllocDetailRequest *request,
      SlurmxGrpc::QueryInteractiveTaskAllocDetailReply *response) override;

  grpc::Status SubmitBatchTask(
      grpc::ServerContext *context,
      const SlurmxGrpc::SubmitBatchTaskRequest *request,
      SlurmxGrpc::SubmitBatchTaskReply *response) override;

  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const SlurmxGrpc::TaskStatusChangeRequest *request,
      SlurmxGrpc::TaskStatusChangeReply *response) override;

  grpc::Status QueryNodeListFromTaskId(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryNodeListFromTaskIdRequest *request,
      SlurmxGrpc::QueryNodeListFromTaskIdReply *response) override;

  grpc::Status CancelTask(grpc::ServerContext *context,
                          const SlurmxGrpc::CancelTaskRequest *request,
                          SlurmxGrpc::CancelTaskReply *response) override;

  grpc::Status QueryJobsInPartition(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryJobsInPartitionRequest *request,
      SlurmxGrpc::QueryJobsInPartitionReply *response) override;

  grpc::Status QueryNodeInfo(grpc::ServerContext *context,
                             const SlurmxGrpc::QueryNodeInfoRequest *request,
                             SlurmxGrpc::QueryNodeInfoReply *response) override;

  grpc::Status QueryPartitionInfo(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryPartitionInfoRequest *request,
      SlurmxGrpc::QueryPartitionInfoReply *response) override;

  grpc::Status AddAccount(grpc::ServerContext *context,
                          const SlurmxGrpc::AddAccountRequest *request,
                          SlurmxGrpc::AddAccountReply *response) override;

  grpc::Status AddUser(grpc::ServerContext *context,
                       const SlurmxGrpc::AddUserRequest *request,
                       SlurmxGrpc::AddUserReply *response) override;

  grpc::Status ModifyEntity(grpc::ServerContext *context,
                            const SlurmxGrpc::ModifyEntityRequest *request,
                            SlurmxGrpc::ModifyEntityReply *response) override;

  grpc::Status QueryEntityInfo(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryEntityInfoRequest *request,
      SlurmxGrpc::QueryEntityInfoReply *response) override;

  grpc::Status DeleteEntity(grpc::ServerContext *context,
                            const SlurmxGrpc::DeleteEntityRequest *request,
                            SlurmxGrpc::DeleteEntityReply *response) override;

  grpc::Status QueryClusterInfo(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryClusterInfoRequest *request,
      SlurmxGrpc::QueryClusterInfoReply *response) override;

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
  explicit CtlXdServer(const Config::SlurmCtlXdListenConf &listen_conf);

  inline void Wait() { m_server_->Wait(); }

  void AddAllocDetailToIaTask(uint32_t task_id,
                              InteractiveTaskAllocationDetail detail)
      LOCKS_EXCLUDED(m_mtx_);

  const InteractiveTaskAllocationDetail *QueryAllocDetailOfIaTask(
      uint32_t task_id) LOCKS_EXCLUDED(m_mtx_);

  void RemoveAllocDetailOfIaTask(uint32_t task_id) LOCKS_EXCLUDED(m_mtx_);

 private:
  using Mutex = util::mutex;
  using LockGuard = util::AbslMutexLockGuard;

  void XdNodeIsUpCb_(XdNodeId node_id);
  void XdNodeIsDownCb_(XdNodeId node_id);

  std::unique_ptr<SlurmCtlXdServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  Mutex m_mtx_;
  // Use absl::hash_node_map because QueryAllocDetailOfIaTask returns a
  // pointer. Pointer stability is needed here. The return type is a const
  // pointer, and it guarantees that the thread safety is not broken.
  absl::node_hash_map<uint32_t /*task id*/, InteractiveTaskAllocationDetail>
      m_task_alloc_detail_map_ GUARDED_BY(m_mtx_);

  inline static std::mutex s_sigint_mtx;
  inline static std::condition_variable s_sigint_cv;
  static void signal_handler_func(int) { s_sigint_cv.notify_one(); };

  friend class SlurmCtlXdServiceImpl;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::CtlXdServer> g_ctlxd_server;