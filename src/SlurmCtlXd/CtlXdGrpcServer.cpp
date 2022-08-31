#include "CtlXdGrpcServer.h"

#include <absl/strings/str_split.h>
#include <google/protobuf/util/time_util.h>
#include <pwd.h>

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

  std::list<std::string> allowed_partition =
      g_mongodb_client->GetUserAllowedPartition(getpwuid(task->uid)->pw_name);
  auto it = std::find(allowed_partition.begin(), allowed_partition.end(),
                      task->partition_name);
  if (it == allowed_partition.end()) {
    response->set_ok(false);
    response->set_reason(fmt::format(
        "The user:{} don't have access to submit task in partition:{}",
        task->uid, task->partition_name));
    return grpc::Status::OK;
  }

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

grpc::Status SlurmCtlXdServiceImpl::AddAccount(
    grpc::ServerContext *context, const SlurmxGrpc::AddAccountRequest *request,
    SlurmxGrpc::AddAccountReply *response) {
  Account account;
  const SlurmxGrpc::AccountInfo *account_info = &request->account();
  account.name = account_info->name();
  account.parent_account = account_info->parent_account();
  account.description = account_info->description();
  account.qos = account_info->qos();
  for (auto &p : account_info->allowed_partition()) {
    account.allowed_partition.emplace_back(p);
  }

  MongodbClient::MongodbResult result = g_mongodb_client->AddAccount(account);
  if (result.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.reason.value());
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::AddUser(
    grpc::ServerContext *context, const SlurmxGrpc::AddUserRequest *request,
    SlurmxGrpc::AddUserReply *response) {
  User user;
  const SlurmxGrpc::UserInfo *user_info = &request->user();
  user.name = user_info->name();
  user.uid = user_info->uid();
  user.account = user_info->account();
  user.admin_level = User::AdminLevel(user_info->admin_level());

  MongodbClient::MongodbResult result = g_mongodb_client->AddUser(user);
  if (result.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.reason.value());
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::ModifyEntity(
    grpc::ServerContext *context,
    const SlurmxGrpc::ModifyEntityRequest *request,
    SlurmxGrpc::ModifyEntityReply *response) {
  MongodbClient::MongodbResult res;

  if (request->NewEntity_case() == request->kNewAccount) {
    const SlurmxGrpc::AccountInfo *new_account = &request->new_account();
    if (!new_account->allowed_partition().empty()) {
      std::list<std::string> partitions;
      for (auto &p : new_account->allowed_partition()) {
        partitions.emplace_back(p);
      }
      if (!g_mongodb_client->SetAccountAllowedPartition(
              new_account->name(), partitions, request->type())) {
        response->set_ok(false);
        response->set_reason("can't update the allowed partitions");
        return grpc::Status::OK;
      }
    }
    Account account;
    account.name = new_account->name();
    account.parent_account = new_account->parent_account();
    account.description = new_account->description();
    account.qos = new_account->qos();
    res = g_mongodb_client->SetAccount(account);
    if (!res.ok) {
      response->set_ok(false);
      response->set_reason(res.reason.value());
      return grpc::Status::OK;
    }
  } else {
    const SlurmxGrpc::UserInfo *new_user = &request->new_user();
    if (!new_user->allowed_partition().empty()) {
      std::list<std::string> partitions;
      for (auto &p : new_user->allowed_partition()) {
        partitions.emplace_back(p);
      }
      if (!g_mongodb_client->SetUserAllowedPartition(
              new_user->name(), partitions, request->type())) {
        response->set_ok(false);
        response->set_reason("can't update the allowed partitions");
        return grpc::Status::OK;
      }
    }
    User user;
    user.name = new_user->name();
    user.uid = new_user->uid();
    user.account = new_user->account();
    user.admin_level = User::AdminLevel(new_user->admin_level());
    res = g_mongodb_client->SetUser(user);
    if (!res.ok) {
      response->set_ok(false);
      response->set_reason(res.reason.value());
      return grpc::Status::OK;
    }
  }
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryEntityInfo(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryEntityInfoRequest *request,
    SlurmxGrpc::QueryEntityInfoReply *response) {
  switch (request->entity_type()) {
    case SlurmxGrpc::Account:
      if (request->name() == "All") {
        std::list<Account> account_list;
        g_mongodb_client->GetAllAccountInfo(account_list);

        auto *list = response->mutable_account_list();
        for (auto &&account : account_list) {
          if (account.deleted) {
            continue;
          }
          auto *account_info = list->Add();
          account_info->set_name(account.name);
          account_info->set_description(account.description);
          auto *user_list = account_info->mutable_users();
          for (auto &&user : account.users) {
            user_list->Add()->assign(user);
          }
          auto *child_list = account_info->mutable_child_account();
          for (auto &&child : account.child_account) {
            child_list->Add()->assign(child);
          }
          account_info->set_parent_account(account.parent_account);
          auto *partition_list = account_info->mutable_allowed_partition();
          for (auto &&partition : account.allowed_partition) {
            partition_list->Add()->assign(partition);
          }
          account_info->set_qos(account.qos);
        }
        response->set_ok(true);
      } else {
        Account account;
        if (g_mongodb_client->GetAccountInfo(request->name(), &account)) {
          auto *account_info = response->mutable_account_list()->Add();
          account_info->set_name(account.name);
          account_info->set_description(account.description);
          auto *user_list = account_info->mutable_users();
          for (auto &&user : account.users) {
            user_list->Add()->assign(user);
          }
          auto *child_list = account_info->mutable_child_account();
          for (auto &&child : account.child_account) {
            child_list->Add()->assign(child);
          }
          account_info->set_parent_account(account.parent_account);
          auto *partition_list = account_info->mutable_allowed_partition();
          for (auto &&partition : account.allowed_partition) {
            partition_list->Add()->assign(partition);
          }
          account_info->set_qos(account.qos);
          response->set_ok(true);
        } else {
          response->set_ok(false);
        }
      }
      break;
    case SlurmxGrpc::User:
      if (request->name() == "All") {
        std::list<User> user_list;
        g_mongodb_client->GetAllUserInfo(user_list);

        auto *list = response->mutable_user_list();
        for (auto &&user : user_list) {
          if (user.deleted) {
            continue;
          }
          auto *user_info = list->Add();
          user_info->set_name(user.name);
          user_info->set_uid(user.uid);
          user_info->set_account(user.account);
          user_info->set_admin_level(
              (SlurmxGrpc::UserInfo_AdminLevel)user.admin_level);
          auto *partition_list = user_info->mutable_allowed_partition();
          for (auto &&partition : user.allowed_partition) {
            partition_list->Add()->assign(partition);
          }
        }
        response->set_ok(true);
      } else {
        User user;
        if (g_mongodb_client->GetUserInfo(request->name(), &user)) {
          auto *user_info = response->mutable_user_list()->Add();
          user_info->set_name(user.name);
          user_info->set_uid(user.uid);
          user_info->set_account(user.account);
          user_info->set_admin_level(
              (SlurmxGrpc::UserInfo_AdminLevel)user.admin_level);
          auto *partition_list = user_info->mutable_allowed_partition();
          for (auto &&partition : user.allowed_partition) {
            partition_list->Add()->assign(partition);
          }
          response->set_ok(true);
        } else {
          response->set_ok(false);
        }
      }
      break;
    case SlurmxGrpc::Qos:
      break;
    default:
      break;
  }
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::DeleteEntity(
    grpc::ServerContext *context,
    const SlurmxGrpc::DeleteEntityRequest *request,
    SlurmxGrpc::DeleteEntityReply *response) {
  MongodbClient::MongodbResult res = g_mongodb_client->DeleteEntity(
      (MongodbClient::EntityType)request->entity_type(), request->name());
  if (res.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(res.reason.value());
  }
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::QueryClusterInfo(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryClusterInfoRequest *request,
    SlurmxGrpc::QueryClusterInfoReply *response) {
  SlurmxGrpc::QueryClusterInfoReply *reply;
  reply = g_meta_container->QueryClusterInfo();
  response->Swap(reply);
  delete reply;

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