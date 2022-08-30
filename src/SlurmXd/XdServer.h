#pragma once

#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "slurmx/Lock.h"

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

#include "TaskManager.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "slurmx/PublicHeader.h"

namespace Xd {

using boost::uuids::uuid;

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using SlurmxGrpc::SlurmXd;
using SlurmxGrpc::SrunXStreamReply;
using SlurmxGrpc::SrunXStreamRequest;

class SlurmXdServiceImpl : public SlurmXd::Service {
 public:
  SlurmXdServiceImpl() = default;

  Status SrunXStream(ServerContext *context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>
                         *stream) override;

  grpc::Status ExecuteTask(grpc::ServerContext *context,
                           const SlurmxGrpc::ExecuteTaskRequest *request,
                           SlurmxGrpc::ExecuteTaskReply *response) override;

  grpc::Status TerminateTask(grpc::ServerContext *context,
                             const SlurmxGrpc::TerminateTaskRequest *request,
                             SlurmxGrpc::TerminateTaskReply *response) override;

  grpc::Status QueryTaskIdFromPort(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryTaskIdFromPortRequest *request,
      SlurmxGrpc::QueryTaskIdFromPortReply *response) override;

  grpc::Status QueryTaskIdFromPortForward(
      grpc::ServerContext *context,
      const SlurmxGrpc::QueryTaskIdFromPortForwardRequest *request,
      SlurmxGrpc::QueryTaskIdFromPortForwardReply *response) override;

  grpc::Status MigrateSshProcToCgroup(
      grpc::ServerContext *context,
      const SlurmxGrpc::MigrateSshProcToCgroupRequest *request,
      SlurmxGrpc::MigrateSshProcToCgroupReply *response) override;

  grpc::Status CreateCgroupForTask(
      grpc::ServerContext *context,
      const SlurmxGrpc::CreateCgroupForTaskRequest *request,
      SlurmxGrpc::CreateCgroupForTaskReply *response) override;

  grpc::Status ReleaseCgroupForTask(
      grpc::ServerContext *context,
      const SlurmxGrpc::ReleaseCgroupForTaskRequest *request,
      SlurmxGrpc::ReleaseCgroupForTaskReply *response) override;
};

class XdServer {
 public:
  explicit XdServer(const Config::SlurmXdListenConf &listen_conf);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

  void GrantResourceToken(const uuid &resource_uuid, uint32_t task_id)
      LOCKS_EXCLUDED(m_mtx_);

  SlurmxErr RevokeResourceToken(const uuid &resource_uuid)
      LOCKS_EXCLUDED(m_mtx_);

  SlurmxErr CheckValidityOfResourceUuid(const uuid &resource_uuid,
                                        uint32_t task_id)
      LOCKS_EXCLUDED(m_mtx_);

 private:
  using Mutex = util::mutex;
  using LockGuard = util::AbslMutexLockGuard;

  // SlurmXd no longer takes the responsibility for resource management.
  // Resource management is handled in SlurmCtlXd. SlurmXd only records
  // who have the permission to execute interactive tasks in SlurmXd.
  // If someone holds a valid resource uuid on a task id, we assume that he
  // has allocated required resource from SlurmCtlXd.
  std::unordered_map<uuid, uint32_t /*task id*/, boost::hash<uuid>>
      m_resource_uuid_map_ GUARDED_BY(m_mtx_);

  Mutex m_mtx_;

  std::unique_ptr<SlurmXdServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class SlurmXdServiceImpl;
};
}  // namespace Xd

// The initialization of XdServer requires some parameters.
// We can't use the Singleton pattern here. So we use one global variable.
inline std::unique_ptr<Xd::XdServer> g_server;
