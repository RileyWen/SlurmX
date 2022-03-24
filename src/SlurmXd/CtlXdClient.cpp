#include "CtlXdClient.h"

#include <boost/uuid/uuid_io.hpp>

namespace Xd {

CtlXdClient::CtlXdClient() {
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

CtlXdClient::~CtlXdClient() {
  m_thread_stop_ = true;
  m_async_send_thread_.join();
}

SlurmxErr CtlXdClient::RegisterOnCtlXd(uint32_t my_port) {
  SlurmXdRegisterRequest req;

  req.set_port(my_port);

  SlurmXdRegisterResult result;

  ClientContext context;
  Status status = m_stub_->RegisterSlurmXd(&context, req, &result);

  if (status.ok()) {
    if (result.ok()) {
      m_node_id_ = {result.node_id().partition_id(),
                    result.node_id().node_index()};
      SLURMX_INFO("Register Node Successfully! Node id: {}", m_node_id_);

      return SlurmxErr::kOk;
    }

    SLURMX_ERROR("Failed to register node. Reason from CtlXd: {}",
                 result.reason());
    return SlurmxErr::kGenericFailure;
  }

  SLURMX_ERROR("Register Failed due to a local error. Code: {}, Msg: {}",
               status.error_code(), status.error_message());
  return SlurmxErr::kGenericFailure;
}

SlurmxErr CtlXdClient::Connect(const std::string& server_address) {
  m_ctlxd_channel_ =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());

  using namespace std::chrono_literals;
  bool ok;
  ok =
      m_ctlxd_channel_->WaitForConnected(std::chrono::system_clock::now() + 3s);
  if (!ok) {
    return SlurmxErr::kConnectionTimeout;
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = SlurmCtlXd::NewStub(m_ctlxd_channel_);

  return SlurmxErr::kOk;
}

void CtlXdClient::TaskStatusChangeAsync(TaskStatusChange&& task_status_change) {
  m_task_status_change_mtx_.Lock();
  m_task_status_change_queue_.emplace(std::move(task_status_change));
  m_task_status_change_mtx_.Unlock();
}

void CtlXdClient::AsyncSendThread_() {
  absl::Condition cond(
      +[](decltype(m_task_status_change_queue_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_queue_);

  TaskStatusChange status_change;

  while (true) {
    if (m_thread_stop_) break;
    bool has_msg = m_task_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(300));
    if (!has_msg) {
      m_task_status_change_mtx_.Unlock();
      continue;
    }

    status_change = std::move(m_task_status_change_queue_.front());
    m_task_status_change_queue_.pop();
    m_task_status_change_mtx_.Unlock();

    grpc::ClientContext context;
    SlurmxGrpc::TaskStatusChangeRequest request;
    SlurmxGrpc::TaskStatusChangeReply reply;
    grpc::Status status;

    request.set_node_index(m_node_id_.node_index);
    request.set_task_id(status_change.task_id);
    switch (status_change.new_status) {
      case SlurmxGrpc::Pending:
        request.set_new_status(SlurmxGrpc::TaskStatus::Pending);
        break;
      case SlurmxGrpc::Running:
        request.set_new_status(SlurmxGrpc::TaskStatus::Running);
        break;
      case SlurmxGrpc::Finished:
        request.set_new_status(SlurmxGrpc::TaskStatus::Finished);
        break;
      case SlurmxGrpc::Failed:
        request.set_new_status(SlurmxGrpc::TaskStatus::Failed);
        break;
      case SlurmxGrpc::Completing:
        request.set_new_status(SlurmxGrpc::TaskStatus::Completing);
        break;
    }
    if (status_change.reason.has_value())
      request.set_reason(status_change.reason.value());

    status = m_stub_->TaskStatusChange(&context, request, &reply);
    if (!status.ok()) {
      SLURMX_ERROR(
          "Failed to send TaskStatusChange: "
          "{{TaskId: {}, NewStatus: {}}}, reason: {}",
          status_change.task_id, status_change.new_status,
          status.error_message());
    }
  }
}

}  // namespace Xd
