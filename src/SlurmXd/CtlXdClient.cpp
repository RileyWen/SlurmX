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

void CtlXdClient::InitChannelAndStub(const std::string& server_address) {
  m_ctlxd_channel_ =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = SlurmCtlXd::NewStub(m_ctlxd_channel_);
}

void CtlXdClient::TaskStatusChangeAsync(TaskStatusChange&& task_status_change) {
  m_task_status_change_mtx_.Lock();
  m_task_status_change_list_.emplace_back(std::move(task_status_change));
  m_task_status_change_mtx_.Unlock();
}

void CtlXdClient::AsyncSendThread_() {
  absl::Condition cond(
      +[](decltype(m_task_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_list_);

  while (true) {
    if (m_thread_stop_) break;
    bool has_msg = m_task_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(300));
    if (!has_msg) {
      m_task_status_change_mtx_.Unlock();
      continue;
    }

    if (m_ctlxd_channel_->GetState(true) == GRPC_CHANNEL_READY) {
      std::list<TaskStatusChange> changes;
      changes.splice(changes.begin(), std::move(m_task_status_change_list_));
      m_task_status_change_mtx_.Unlock();

      while (!changes.empty()) {
        grpc::ClientContext context;
        SlurmxGrpc::TaskStatusChangeRequest request;
        SlurmxGrpc::TaskStatusChangeReply reply;
        grpc::Status status;

        auto status_change = changes.front();

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
          default:
            break;
        }
        if (status_change.reason.has_value())
          request.set_reason(status_change.reason.value());

        SLURMX_TRACE("Sending TaskStatusChange for task #{}",
                     status_change.task_id);
        status = m_stub_->TaskStatusChange(&context, request, &reply);
        if (!status.ok()) {
          SLURMX_ERROR(
              "Failed to send TaskStatusChange: "
              "{{TaskId: {}, NewStatus: {}}}, reason: {}, code: {}",
              status_change.task_id, status_change.new_status,
              status.error_message(), status.error_code());

          if (status.error_code() == grpc::UNAVAILABLE) {
            // If some messages are not sent due to channel failure,
            // put them back into m_task_status_change_list_
            if (!changes.empty()) {
              m_task_status_change_mtx_.Lock();
              m_task_status_change_list_.splice(
                  m_task_status_change_list_.begin(), std::move(changes));
              m_task_status_change_mtx_.Unlock();
            }
            break;
          }
        } else {
          changes.pop_front();
        }
      }
    } else {
      m_task_status_change_mtx_.Unlock();
    }
  }
}

}  // namespace Xd
