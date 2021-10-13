#include "CtlXdGrpcServer.h"

#include <signal.h>

#include <limits>
#include <utility>

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

  XdNodeId node_id{};
  node_id.partition_id =
      g_meta_container->GetPartitionId(request->partition_name());
  node_id.node_index =
      g_meta_container->AllocNodeIndexInPartition(node_id.partition_id);

  Resources res_total;
  res_total.allocatable_resource =
      request->resource_total().allocatable_resource();
  std::future<RegisterNodeResult> result_future = g_node_keeper->RegisterXdNode(
      addr_port, node_id,
      new XdNodeStaticMeta{.node_index = node_id.node_index,
                           .ipv4_addr = peer_slices[1],
                           .port = request->port(),
                           .node_name = request->node_name(),
                           .partition_id = node_id.partition_id,
                           .partition_name = request->partition_name(),
                           .res = res_total},
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
  interactive_task->time_limit = absl::Seconds(request->time_limit_sec());
  interactive_task->type = ITask::Type::Interactive;

  // Todo: Eliminate useless allocation here when err!=kOk.
  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(interactive_task), &task_id);

  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_task_id(task_id);
  } else {
    response->set_ok(false);
    response->set_reason(err == SlurmxErr::kNonExistent
                             ? "Partition doesn't exist!"
                             : "Resource not enough!");
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::SubmitBatchTask(
    grpc::ServerContext *context,
    const SlurmxGrpc::SubmitBatchTaskRequest *request,
    SlurmxGrpc::SubmitBatchTaskReply *response) {
  SlurmxErr err;

  auto task = std::make_unique<BatchTask>();
  task->partition_name = request->partition_name();
  task->resources.allocatable_resource =
      request->required_resources().allocatable_resource();
  task->time_limit = absl::Seconds(request->time_limit().seconds());

  task->executive_path = request->executive_path();
  for (auto &&arg : request->arguments()) {
    task->arguments.push_back(arg);
  }
  task->output_file_pattern = request->output_file_pattern();

  task->type = ITask::Type::Batch;

  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(task), &task_id);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_task_id(task_id);
  } else {
    response->set_ok(false);
    response->set_reason(err == SlurmxErr::kNonExistent
                             ? "Partition doesn't exist!"
                             : "Resource not enough!");
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryInteractiveTaskAllocDetail(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryInteractiveTaskAllocDetailRequest *request,
    SlurmxGrpc::QueryInteractiveTaskAllocDetailReply *response) {
  auto *detail = g_ctlxd_server->QueryAllocDetailOfIaTask(request->task_id());
  if (detail) {
    response->set_ok(true);
    response->mutable_detail()->set_ipv4_addr(detail->ipv4_addr);
    response->mutable_detail()->set_port(detail->port);
    response->mutable_detail()->set_node_index(detail->node_index);
    response->mutable_detail()->set_resource_uuid(detail->resource_uuid.data,
                                                  detail->resource_uuid.size());
  } else {
    response->set_ok(false);
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::TaskStatusChange(
    grpc::ServerContext *context,
    const SlurmxGrpc::TaskStatusChangeRequest *request,
    SlurmxGrpc::TaskStatusChangeReply *response) {
  ITask::Status status{};
  if (request->new_status() == SlurmxGrpc::Finished)
    status = ITask::Status::Finished;
  else if (request->new_status() == SlurmxGrpc::Failed)
    status = ITask::Status::Failed;
  else
    SLURMX_ERROR(
        "Task #{}: When TaskStatusChange RPC is called, the task should either "
        "be Finished or Failed.",
        request->task_id());

  std::optional<std::string> reason;
  if (!request->reason().empty()) reason = request->reason();

  g_task_scheduler->TaskStatusChange(request->task_id(), status, reason);

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::TerminateTask(
    grpc::ServerContext *context,
    const SlurmxGrpc::TerminateTaskRequest *request,
    SlurmxGrpc::TerminateTaskReply *response) {
  uint32_t task_id = request->task_id();

  XdNodeId xd_node_id;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  bool ok = g_task_scheduler->QueryXdNodeIdOfRunningTask(task_id, &xd_node_id);

  if (ok) {
    auto stub = g_node_keeper->GetXdStub(xd_node_id);
    SlurmxErr err = stub->TerminateTask(task_id);
    if (err == SlurmxErr::kOk)
      response->set_ok(true);
    else {
      // Todo: make the reason be set here!
      response->set_ok(false);
    }
  }

  return grpc::Status::OK;
}

CtlXdServer::CtlXdServer(std::string listen_address)
    : m_listen_address_(std::move(listen_address)) {
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

  SLURMX_INFO(
      "Node {} registered. cpu: {}, mem: {}, mem+sw: {}", node_id,
      static_meta->res.allocatable_resource.cpu_count,
      util::ReadableMemory(static_meta->res.allocatable_resource.memory_bytes),
      util::ReadableMemory(
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

void CtlXdServer::AddAllocDetailToIaTask(
    uint32_t task_id, InteractiveTaskAllocationDetail detail) {
  LockGuard guard(m_mtx_);
  m_task_alloc_detail_map_.emplace(task_id, std::move(detail));
}

const InteractiveTaskAllocationDetail *CtlXdServer::QueryAllocDetailOfIaTask(
    uint32_t task_id) {
  LockGuard guard(m_mtx_);
  auto iter = m_task_alloc_detail_map_.find(task_id);
  if (iter == m_task_alloc_detail_map_.end()) return nullptr;

  return &iter->second;
}

void CtlXdServer::RemoveAllocDetailOfIaTask(uint32_t task_id) {
  LockGuard guard(m_mtx_);
  m_task_alloc_detail_map_.erase(task_id);
}

}  // namespace CtlXd