#include "CtlXdGrpcServer.h"

#include <signal.h>

#include <limits>

#include "TaskScheduler.h"
#include "XdNodeKeeper.h"
#include "XdNodeMetaContainer.h"
#include "slurmx/String.h"

namespace CtlXd {

grpc::Status CtlXd::SlurmCtlXdServiceImpl::RegisterSlurmXd(
    grpc::ServerContext *context,
    const SlurmxGrpc::SlurmXdRegisterRequest *request,
    SlurmxGrpc::SlurmXdRegisterResult *response) {
  std::string peer = context->peer();
  std::vector<std::string> peer_slices;
  // ["ipv4", "<address>, "<port>"]
  boost::algorithm::split(peer_slices, peer, boost::is_any_of(":"));

  std::string addr_port = fmt::format("{}:{}", peer_slices[1], request->port());

  XdNodeId node_id;
  node_id.partition_id =
      g_meta_container->GetPartitionId(request->partition_name());
  node_id.node_index =
      g_meta_container->AllocNodeIndexInPartition(node_id.partition_id);

  std::future<RegisterNodeResult> result_future = g_node_keeper->RegisterXdNode(
      addr_port, node_id,
      new XdNodeStaticMeta{.node_index = node_id.node_index,
                           .ipv4_addr = peer_slices[1],
                           .port = request->port(),
                           .node_name = request->node_name(),
                           .partition_id = node_id.partition_id,
                           .partition_name = request->partition_name(),
                           .res = {request->resource_total()}},
      [](void *data) { delete reinterpret_cast<AllocatableResource *>(data); });

  RegisterNodeResult result = result_future.get();

  if (result.node_id.has_value()) {
    response->set_ok(true);
    response->mutable_node_id()->set_partition_id(
        result.node_id.value().partition_id);
    response->mutable_node_id()->set_node_index(
        result.node_id.value().node_index);
  } else {
    response->set_ok(false);
    response->set_reason("CtlXd cannot connect to Xd backward");

    SLURMX_TRACE("Failed to establish the backward channel to XdClient {}.",
                 addr_port);

    // Avoid memory leak.
    g_meta_container->TryReleasePartition(node_id.partition_id);
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::AllocateInteractiveTask(
    grpc::ServerContext *context,
    const SlurmxGrpc::InteractiveTaskAllocRequest *request,
    SlurmxGrpc::InteractiveTaskAllocReply *response) {
  SlurmxErr err;
  auto interactive_task = std::make_unique<InteractiveTask>();

  interactive_task->partition_name = request->partition_name();
  interactive_task->resources.allocatable_resource =
      request->required_resources().allocatable_resource();
  interactive_task->time_limit_sec = request->time_limit_sec();
  interactive_task->type = ITask::Type::Interactive;

  // Todo: Eliminate useless allocation here when err!=kOk.
  auto task_meta = std::make_unique<BasicTaskMeta>();

  err = g_task_scheduler->SubmitTask(std::move(interactive_task),
                                     task_meta.get());

  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->mutable_task_meta()->set_resource_uuid(
        task_meta->resource_uuid.data, task_meta->resource_uuid.size());
    response->mutable_task_meta()->set_task_id(task_meta->task_id);
  } else {
    response->set_ok(false);
    response->set_reason(err == SlurmxErr::kNonExistent
                             ? "Partition doesn't exist!"
                             : "Resource not enough!");
  }

  return Service::AllocateInteractiveTask(context, request, response);
}

grpc::Status SlurmCtlXdServiceImpl::DeallocateResource(
    grpc::ServerContext *context,
    const SlurmxGrpc::DeallocateResourceRequest *request,
    SlurmxGrpc::DeallocateResourceReply *response) {
  SlurmxErr err;
  uuid res_uuid;

  std::copy(request->resource_uuid().begin(), request->resource_uuid().end(),
            res_uuid.data);
  err = m_ctlxd_server_->DeallocateResource(
      XdNodeId{request->node_id().partition_id(),
               request->node_id().node_index()},
      res_uuid);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(
        "Node index or resource uuid does not exist. Resource Deallocation "
        "failed.");
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::Heartbeat(
    grpc::ServerContext *context, const SlurmxGrpc::HeartbeatRequest *request,
    SlurmxGrpc::HeartbeatReply *response) {
  uuid node_uuid;
  std::copy(request->node_uuid().begin(), request->node_uuid().end(),
            node_uuid.data);

  m_ctlxd_server_->HeartBeatFromNode(node_uuid);

  return grpc::Status::OK;
}

CtlXdServer::CtlXdServer(const std::string &listen_address)
    : m_listen_address_(listen_address) {
  m_service_impl_ = std::make_unique<SlurmCtlXdServiceImpl>(this);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(m_listen_address_,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  SLURMX_INFO("SlurmCtlXd is listening on {}", m_listen_address_);

  // Avoid the potential deadlock error in underlying absl::mutex
  std::thread sigint_waiting_thread([p_server = m_server_.get()] {
    std::unique_lock<std::mutex> lk(s_sigint_mtx);
    s_sigint_cv.wait(lk);

    SLURMX_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");
    p_server->Shutdown();
  });
  sigint_waiting_thread.detach();

  signal(SIGINT, &CtlXdServer::signal_handler_func);

  g_node_keeper->SetNodeIsUpCb(std::bind(&CtlXdServer::XdNodeIsUpCb_, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2));

  g_node_keeper->SetNodeIsDownCb(std::bind(&CtlXdServer::XdNodeIsDownCb_, this,
                                           std::placeholders::_1,
                                           std::placeholders::_2));
}

void CtlXdServer::XdNodeIsUpCb_(XdNodeId node_id, void *node_data) {
  SLURMX_TRACE(
      "A new node #{} is up now. Add its resource to the global resource pool.",
      node_id);

  XdNodeStub *xd_stub = g_node_keeper->GetXdStub(node_id);
  SLURMX_ASSERT(xd_stub != nullptr,
                "Got nullptr of XdNodeStub in NodeIsUp() callback!");

  auto *static_meta = reinterpret_cast<XdNodeStaticMeta *>(node_data);

  g_meta_container->AddNode(*static_meta);

  SLURMX_INFO("Node {} registered. cpu: {}, mem: {}, mem+sw: {}", node_id,
              static_meta->res.allocatable_resource.cpu_count,
              slurmx::ReadableMemory(
                  static_meta->res.allocatable_resource.memory_bytes),
              slurmx::ReadableMemory(
                  static_meta->res.allocatable_resource.memory_sw_bytes));

  // Delete node_data(node_res) (allocated in RegisterSlurmXd) here because it's
  // useless now. The resource information is now kept in global MetaContainer.
  // Set it to nullptr, so next delete call in clean_up_cb will not delete it
  // again.
  xd_stub->SetNodeData(nullptr);
  delete static_meta;
}

void CtlXdServer::XdNodeIsDownCb_(XdNodeId node_id, void *) {
  SLURMX_TRACE(
      "XdNode #{} is down now. Remove its resource from the global resource "
      "pool.",
      node_id);

  g_meta_container->DeleteNodeMeta(node_id);
}

// SlurmxErr CtlXdServer::AllocateResource(const std::string &partition_name,
//                                         const AllocatableResource &res,
//                                         SlurmxGrpc::ResourceInfo *res_info) {
//   SLURMX_TRACE("Trying Allocating resource: cpu {}, mem: {}, mem+sw: {}",
//                res.cpu_count, slurmx::ReadableMemory(res.memory_bytes),
//                slurmx::ReadableMemory(res.memory_sw_bytes));
//
//   if (!g_meta_container->PartitionExists(partition_name)) {
//     SLURMX_DEBUG("Partition {} doesn't exist. Resource allocation failed.",
//                  partition_name);
//     return SlurmxErr::kNonExistent;
//   }
//
//   uint32_t partition_id = g_meta_container->GetPartitionId(partition_name);
//
//   auto metas_ptr = g_meta_container->GetPartitionMetasPtr(partition_id);
//   if (metas_ptr->partition_global_meta.m_resource_avail_ < res) {
//     SLURMX_TRACE(
//         "Resource not enough. Avail: cpu {}, mem: {}, mem+sw: {}",
//         metas_ptr->partition_global_meta.m_resource_avail_.cpu_count,
//         slurmx::ReadableMemory(
//             metas_ptr->partition_global_meta.m_resource_avail_.memory_bytes),
//         slurmx::ReadableMemory(metas_ptr->partition_global_meta
//                                    .m_resource_avail_.memory_sw_bytes));
//     return SlurmxErr::kNoResource;
//   }
//
//   for (auto &[index, node] : metas_ptr->xd_node_meta_map) {
//     bool node_valid = g_node_keeper->XdNodeValid(index);
//     SLURMX_TRACE("Node #{} Valid: {}", index, node_valid);
//     if (node_valid && res <= node.res_avail) {
//       // Todo: We should query the node to test if the required resource
//       //  does not exceed the remaining resource on the node.
//       //  Slurm seems to work in this way.
//       uuid res_uuid;
//
//       res_uuid = m_uuid_gen_();
//       node.res_in_use += res;
//       node.res_avail -= res;
//
//       node.resource_shards.emplace(res_uuid, res);
//
//       metas_ptr->partition_global_meta.m_resource_in_use_ += res;
//       metas_ptr->partition_global_meta.m_resource_avail_ -= res;
//
//       SLURMX_TRACE(
//           "Resource allocated successfully. Node index: {}. uuid: {}. "
//           "Informing XdClient...",
//           index, to_string(res_uuid));
//
//       XdNodeStub *xd_stub =
//           g_node_keeper->GetXdStub(XdNodeId{partition_id, index});
//       SLURMX_ASSERT(xd_stub != nullptr,
//                     "XdNode {}'s stub pointer shouldn't be nullptr when it's
//                     " "valid in bitset.");
//
//       res_info->set_node_index(index);
//       *res_info->mutable_ipv4_addr() = node.static_meta.ipv4_addr;
//       res_info->set_port(node.static_meta.port);
//       res_info->mutable_resource_uuid()->assign(res_uuid.begin(),
//                                                 res_uuid.end());
//
//       return xd_stub->GrantResourceToken(res_uuid, res);
//     }
//   }
//
//   return SlurmxErr::kNoResource;
// }

SlurmxErr CtlXdServer::DeallocateResource(XdNodeId node_id,
                                          const uuid &resource_uuid) {
  SLURMX_TRACE("Trying Deallocating resource uuid in Node {}: {}", node_id,
               boost::uuids::to_string(resource_uuid));
  auto meta_ptr = g_meta_container->GetNodeMetaPtr(node_id);
  if (!meta_ptr) {
    SLURMX_DEBUG("Node {} not found in xd_node_meta_map", node_id);
    return SlurmxErr::kNonExistent;
  }

  XdNodeMeta &node_meta = *meta_ptr;
  auto shard_iter = node_meta.resource_shards.find(resource_uuid);
  if (shard_iter == node_meta.resource_shards.end()) {
    SLURMX_DEBUG("resource uuid {} not found in Node #{}'s shards",
                 boost::uuids::to_string(resource_uuid), node_id);
    return SlurmxErr::kNonExistent;
  }

  // Note: Recursive lock here!
  auto part_metas =
      g_meta_container->GetPartitionMetasPtr(node_id.partition_id);

  // Modify partition meta
  part_metas->partition_global_meta.m_resource_in_use_ -= shard_iter->second;
  part_metas->partition_global_meta.m_resource_avail_ += shard_iter->second;

  // Modify node meta
  node_meta.res_in_use -= shard_iter->second;
  node_meta.res_avail += shard_iter->second;
  node_meta.resource_shards.erase(shard_iter);

  return SlurmxErr::kOk;
}

void CtlXdServer::HeartBeatFromNode(const uuid &node_uuid) {}

}  // namespace CtlXd