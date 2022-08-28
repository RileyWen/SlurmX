#include "CtlXdGrpcServer.h"

#include <google/protobuf/util/time_util.h>

#include <csignal>
#include <limits>
#include <utility>

#include "TaskScheduler.h"
#include "XdNodeKeeper.h"
#include "XdNodeMetaContainer.h"
#include "slurmx/Network.h"
#include "slurmx/String.h"

namespace CtlXd {

grpc::Status SlurmCtlXdServiceImpl::AllocateInteractiveTask(
    grpc::ServerContext *context,
    const SlurmxGrpc::InteractiveTaskAllocRequest *request,
    SlurmxGrpc::InteractiveTaskAllocReply *response) {
  SlurmxErr err;
  auto task = std::make_unique<TaskInCtlXd>();

  task->partition_name = request->partition_name();
  task->resources.allocatable_resource =
      request->required_resources().allocatable_resource();
  task->time_limit = absl::Seconds(request->time_limit_sec());
  task->type = SlurmxGrpc::Interactive;
  task->meta = InteractiveMetaInTask{};

  // Todo: Eliminate useless allocation here when err!=kOk.
  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(task), false, &task_id);

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

  auto task = std::make_unique<TaskInCtlXd>();
  task->partition_name = request->task().partition_name();
  task->resources.allocatable_resource =
      request->task().resources().allocatable_resource();
  task->time_limit = absl::Seconds(request->task().time_limit().seconds());

  task->meta = BatchMetaInTask{};
  auto &batch_meta = std::get<BatchMetaInTask>(task->meta);
  batch_meta.sh_script = request->task().batch_meta().sh_script();
  batch_meta.output_file_pattern =
      request->task().batch_meta().output_file_pattern();

  task->type = SlurmxGrpc::Batch;

  task->node_num = request->task().node_num();
  task->ntasks_per_node = request->task().ntasks_per_node();
  task->cpus_per_task = request->task().cpus_per_task();

  task->uid = request->task().uid();
  task->name = request->task().name();
  task->cmd_line = request->task().cmd_line();
  task->env = request->task().env();
  task->cwd = request->task().cwd();

  task->task_to_ctlxd = request->task();

  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(task), false, &task_id);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_task_id(task_id);
    SLURMX_DEBUG("Received an batch task request. Task id allocated: {}",
                 task_id);
  } else if (err == SlurmxErr::kNonExistent) {
    response->set_ok(false);
    response->set_reason("Partition doesn't exist!");
    SLURMX_DEBUG(
        "Received an batch task request "
        "but the allocation failed. Reason: Resource "
        "not enough!");
  } else if (err == SlurmxErr::kInvalidNodeNum) {
    response->set_ok(false);
    response->set_reason(
        "--node is either invalid or greater than "
        "the number of alive nodes in its partition.");
    SLURMX_DEBUG(
        "Received an batch task request "
        "but the allocation failed. Reason: --node is either invalid or "
        "greater than the number of alive nodes in its partition.");
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
  SlurmxGrpc::TaskStatus status{};
  if (request->new_status() == SlurmxGrpc::Finished)
    status = SlurmxGrpc::Finished;
  else if (request->new_status() == SlurmxGrpc::Failed)
    status = SlurmxGrpc::Failed;
  else if (request->new_status() == SlurmxGrpc::Cancelled)
    status = SlurmxGrpc::Cancelled;
  else
    SLURMX_ERROR(
        "Task #{}: When TaskStatusChange RPC is called, the task should either "
        "be Finished, Failed or Cancelled. new_status = {}",
        request->task_id(), request->new_status());

  std::optional<std::string> reason;
  if (!request->reason().empty()) reason = request->reason();

  g_task_scheduler->TaskStatusChange(request->task_id(), request->node_index(),
                                     status, reason);
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::CancelTask(
    grpc::ServerContext *context, const SlurmxGrpc::CancelTaskRequest *request,
    SlurmxGrpc::CancelTaskReply *response) {
  uint32_t task_id = request->task_id();

  SlurmxErr err = g_task_scheduler->CancelPendingOrRunningTask(task_id);
  // Todo: make the reason be set here!
  if (err == SlurmxErr::kOk)
    response->set_ok(true);
  else {
    response->set_ok(false);
    if (err == SlurmxErr::kNonExistent)
      response->set_reason("Task id doesn't exist!");
    else
      response->set_reason(SlurmxErrStr(err).data());
  }
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryNodeInfo(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryNodeInfoRequest *request,
    SlurmxGrpc::QueryNodeInfoReply *response) {
  SlurmxGrpc::QueryNodeInfoReply *reply;

  if (request->node_name().empty()) {
    reply = g_meta_container->QueryAllNodeInfo();
    response->Swap(reply);
    delete reply;
  } else {
    reply = g_meta_container->QueryNodeInfo(request->node_name());
    response->Swap(reply);
    delete reply;
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryPartitionInfo(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryPartitionInfoRequest *request,
    SlurmxGrpc::QueryPartitionInfoReply *response) {
  SlurmxGrpc::QueryPartitionInfoReply *reply;

  if (request->partition_name().empty()) {
    reply = g_meta_container->QueryAllPartitionInfo();
    response->Swap(reply);
    delete reply;
  } else {
    reply = g_meta_container->QueryPartitionInfo(request->partition_name());
    response->Swap(reply);
    delete reply;
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryJobsInPartition(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryJobsInPartitionRequest *request,
    SlurmxGrpc::QueryJobsInPartitionReply *response) {
  uint32_t partition_id;

  if (!g_meta_container->GetPartitionId(request->partition(), &partition_id))
    return grpc::Status::OK;
  g_task_scheduler->QueryTaskBriefMetaInPartition(
      partition_id, QueryBriefTaskMetaFieldControl{true, true, true, true},
      response);

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryNodeListFromTaskId(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryNodeListFromTaskIdRequest *request,
    SlurmxGrpc::QueryNodeListFromTaskIdReply *response) {
  auto node_list =
      g_task_scheduler->QueryNodeListFromTaskId(request->task_id());
  if (!node_list.empty()) {
    response->set_ok(true);
    response->set_node_list(node_list);
  } else {
    response->set_ok(false);
  }
  return grpc::Status::OK;
}

CtlXdServer::CtlXdServer(const Config::SlurmCtlXdListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<SlurmCtlXdServiceImpl>(this);

  std::string listen_addr_port =
      fmt::format("{}:{}", listen_conf.SlurmCtlXdListenAddr,
                  listen_conf.SlurmCtlXdListenPort);

  grpc::ServerBuilder builder;
  if (listen_conf.UseTls) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair pem_key_cert_pair;
    pem_key_cert_pair.cert_chain = listen_conf.CertContent;
    pem_key_cert_pair.private_key = listen_conf.KeyContent;

    grpc::SslServerCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = listen_conf.CertContent;
    ssl_opts.pem_key_cert_pairs.emplace_back(std::move(pem_key_cert_pair));
    ssl_opts.force_client_auth = true;
    ssl_opts.client_certificate_request =
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    builder.AddListeningPort(listen_addr_port,
                             grpc::SslServerCredentials(ssl_opts));
  } else {
    builder.AddListeningPort(listen_addr_port,
                             grpc::InsecureServerCredentials());
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    SLURMX_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }

  SLURMX_INFO("SlurmCtlXd is listening on {} and Tls is {}", listen_addr_port,
              listen_conf.UseTls);

  // Avoid the potential deadlock error in underlying absl::mutex
  std::thread sigint_waiting_thread([p_server = m_server_.get()] {
    std::unique_lock<std::mutex> lk(s_sigint_mtx);
    s_sigint_cv.wait(lk);

    SLURMX_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");
    p_server->Shutdown();
  });
  sigint_waiting_thread.detach();

  signal(SIGINT, &CtlXdServer::signal_handler_func);

  g_node_keeper->SetNodeIsUpCb(
      std::bind(&CtlXdServer::XdNodeIsUpCb_, this, std::placeholders::_1));

  g_node_keeper->SetNodeIsDownCb(
      std::bind(&CtlXdServer::XdNodeIsDownCb_, this, std::placeholders::_1));
}

void CtlXdServer::XdNodeIsUpCb_(XdNodeId node_id) {
  SLURMX_TRACE(
      "A new node #{} is up now. Add its resource to the global resource pool.",
      node_id);

  XdNodeStub *xd_stub = g_node_keeper->GetXdStub(node_id);
  SLURMX_ASSERT_MSG(xd_stub != nullptr,
                    "Got nullptr of XdNodeStub in NodeIsUp() callback!");

  g_meta_container->NodeUp(node_id);

  SLURMX_INFO("Node {} is up.", node_id);
}

void CtlXdServer::XdNodeIsDownCb_(XdNodeId node_id) {
  SLURMX_TRACE(
      "XdNode #{} is down now. Remove its resource from the global resource "
      "pool.",
      node_id);

  g_meta_container->NodeDown(node_id);
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