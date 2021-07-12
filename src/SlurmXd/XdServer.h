#pragma once

#include <grpc++/grpc++.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

#include "PublicHeader.h"
#include "TaskManager.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace Xd {

using boost::uuids::uuid;

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using slurmx_grpc::GrantResourceTokenReply;
using slurmx_grpc::GrantResourceTokenRequest;
using slurmx_grpc::RevokeResourceTokenReply;
using slurmx_grpc::RevokeResourceTokenRequest;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;

class SlurmXdServiceImpl : public SlurmXd::Service {
 public:
  SlurmXdServiceImpl() = default;

  Status SrunXStream(ServerContext *context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>
                         *stream) override;

  Status GrantResourceToken(ServerContext *context,
                            const GrantResourceTokenRequest *request,
                            GrantResourceTokenReply *response) override;

  Status RevokeResourceToken(ServerContext *context,
                             const RevokeResourceTokenRequest *request,
                             RevokeResourceTokenReply *response) override;
};

class XdServer {
 public:
  XdServer(const std::string &listen_address, const resource_t &total_resource);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

  SlurmxErr GrantResourceToken(const uuid &resource_uuid,
                               const resource_t &required_resource);

  SlurmxErr RevokeResourceToken(const uuid &resource_uuid);

  SlurmxErr CheckValidityOfResourceUuid(const uuid &resource_uuid);

  std::optional<resource_t> FindResourceByUuid(const uuid &resource_uuid);

 private:
  uint64_t NewTaskSeqNum() { return m_task_seq_++; };

  // total = avail + in-use
  resource_t m_resource_total_;
  resource_t m_resource_avail_;
  resource_t m_resource_in_use_;

  // It is used to record allocated resources (from slurmctlxd)
  //  in this node.
  std::unordered_map<uuid, resource_t, boost::hash<uuid>> m_resource_uuid_map_;

  // The mutex which protects the accounting of resource on this node.
  std::mutex m_node_resource_mtx_;

  std::atomic_uint64_t m_task_seq_;

  const std::string m_listen_address_;

  std::unique_ptr<SlurmXdServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class SlurmXdServiceImpl;
};
}  // namespace Xd

// The initialization of XdServer requires some parameters.
// We can't use the Singleton pattern here. So we use one global variable.
inline std::unique_ptr<Xd::XdServer> g_server;
