#include "XdServer.h"

namespace Xd {

using boost::uuids::uuid;

Status SlurmXdServiceImpl::SrunXStream(
    ServerContext *context,
    ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest> *stream) {
  SLURMX_DEBUG("SrunX connects from {}", context->peer());

  enum class StreamState {
    kNegotiation = 0,
    kNewTask,
    kWaitForEofOrSigOrTaskEnd,
    kFinish,
    kAbort
  };

  bool ok;
  SrunXStreamRequest request;
  SrunXStreamReply reply;

  StreamState state = StreamState::kNegotiation;
  while (true) {
    switch (state) {
      case StreamState::kNegotiation:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() !=
              slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
            SLURMX_DEBUG("Expect negotiation from peer {}, but none.",
                         context->peer());
            state = StreamState::kAbort;
          } else {
            state = StreamState::kNewTask;
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }
        break;

      case StreamState::kNewTask:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
            SLURMX_DEBUG("Expect new_task from peer {}, but none.",
                         context->peer());
            state = StreamState::kAbort;
          } else {
            uuid resource_uuid;
            std::copy(request.task_info().resource_uuid().begin(),
                      request.task_info().resource_uuid().end(),
                      resource_uuid.data);

            // Check the validity of resource uuid provided by client.
            SlurmxErr err;
            err = g_server->CheckValidityOfResourceUuid(resource_uuid);

            if (err != SlurmxErr::kOk) {
              // The resource uuid provided by Client is invalid. Reject.

              reply.Clear();
              reply.set_type(slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

              slurmx_grpc::NewTaskResult *result;
              result->set_ok(false);
              result->set_reason(std::string(SlurmxErrStr(err)));

              stream->WriteLast(reply, grpc::WriteOptions());

              state = StreamState::kFinish;
            } else {
              std::string task_name = fmt::format("{}_{}", context->peer(),
                                                  g_server->NewTaskSeqNum());

              std::forward_list<std::string> arguments;
              auto iter = arguments.before_begin();
              for (const auto &arg : request.task_info().arguments()) {
                iter = arguments.emplace_after(iter, arg);
              }

              // We have checked the validity of resource uuid.
              std::optional<resource_t> resource_limit =
                  g_server->FindResourceByUuid(resource_uuid);

              CgroupLimit cg_limit{
                  .cpu_core_limit = resource_limit.value().cpu_count,
                  .memory_limit_bytes = resource_limit.value().memory_bytes,
                  .memory_sw_limit_bytes =
                      resource_limit.value().memory_sw_bytes};

              // It's safe to call stream->Write() on a closed stream.
              // (stream->Write() just return false rather than throwing an
              // exception).
              auto output_callback = [stream](std::string &&buf) {
                slurmx_grpc::SrunXStreamReply reply;
                reply.set_type(
                    slurmx_grpc::SrunXStreamReply_Type_IoRedirection);

                std::string *reply_buf =
                    reply.mutable_io_redirection()->mutable_buf();
                *reply_buf = std::move(buf);

                stream->Write(reply);
              };

              // Call stream->WriteLast() and cause the grpc thread
              // that owns 'stream' to stop the connection handling and quit.
              auto finish_callback = [stream](bool is_terminated_by_signal,
                                              int value) {
                slurmx_grpc::SrunXStreamReply reply;
                reply.set_type(slurmx_grpc::SrunXStreamReply_Type_ExitStatus);

                slurmx_grpc::TaskExitStatus *stat =
                    reply.mutable_task_exit_status();
                stat->set_reason(
                    is_terminated_by_signal
                        ? slurmx_grpc::TaskExitStatus_ExitReason_Signal
                        : slurmx_grpc::TaskExitStatus_ExitReason_Normal);
                stat->set_value(value);

                stream->WriteLast(reply, grpc::WriteOptions());
              };

              TaskInitInfo task_info{
                  .name = std::move(task_name),
                  .executive_path = request.task_info().executive_path(),
                  .arguments = arguments,
                  .cg_limit = cg_limit,
                  .output_callback = std::move(output_callback),
                  .finish_callback = std::move(finish_callback),
              };

              TaskManager::GetInstance().AddTaskAsync(std::move(task_info));

              state = StreamState::kWaitForEofOrSigOrTaskEnd;
            }
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }
        break;

      case StreamState::kWaitForEofOrSigOrTaskEnd: {
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_Signal) {
            SLURMX_DEBUG("Expect signal from peer {}, but none.",
                         context->peer());
            state = StreamState::kAbort;
          } else {
            if (request.signal().signal_type() ==
                slurmx_grpc::Signal_SignalType_Interrupt) {
              // If ctrl+C is pressed before the task ends, inform TaskManager
              //  of the interrupt and wait for TaskManager to stop the Task.

              // Todo: Not Implemented: Send signal to task manager here and
              //  wait for result.

              slurmx_grpc::TaskExitStatus *exit_status =
                  reply.mutable_task_exit_status();
              exit_status->set_reason(
                  slurmx_grpc::TaskExitStatus_ExitReason_Signal);
              exit_status->set_value(9);

              reply.set_allocated_task_exit_status(exit_status);
              stream->WriteLast(reply, grpc::WriteOptions());

              state = StreamState::kFinish;
            } else
              state = StreamState::kAbort;
          }
        } else {
          // If the task ends before user sends a ctrl+C interrupt,
          // the callback which handles the end of a task in TaskManager will
          // call stream->WriteLast() to end the stream. Then the stream->Read()
          // returns with ok = false.

          SLURMX_DEBUG(
              "Stream with WaitForEofOrSigOrTaskEnd on Connection of peer {} "
              "has no more message to read. Ending...",
              context->peer());

          state = StreamState::kFinish;
        }
        break;
      }

      case StreamState::kAbort:
        SLURMX_DEBUG("Connection from peer {} aborted.", context->peer());
        return Status::CANCELLED;

      case StreamState::kFinish:
        SLURMX_TRACE("Connection from peer {} finished normally",
                     context->peer());
        return Status::OK;

      default:
        SLURMX_ERROR("Unexpected XdServer State: {}", state);
        return Status::CANCELLED;
    }
  }
}

SlurmxErr XdServer::GrantResourceToken(const uuid &resource_uuid,
                                       const resource_t &required_resource) {
  if (required_resource <= m_resource_avail_) {
    SLURMX_DEBUG("Resource allocated successfully on uuid {}",
                 to_string(resource_uuid));

    m_node_resource_mtx_.lock();

    m_resource_avail_ -= required_resource;
    m_resource_in_use_ += required_resource;

    m_resource_uuid_map_.emplace(resource_uuid, required_resource);

    m_node_resource_mtx_.unlock();

    return SlurmxErr::kOk;
  }

  SLURMX_DEBUG("Failed to allocate resource on uuid {}",
               to_string(resource_uuid));
  return SlurmxErr::kNoResource;
}

SlurmxErr XdServer::RevokeResourceToken(const uuid &resource_uuid) {
  std::lock_guard<std::mutex> lock_guard(m_node_resource_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter == m_resource_uuid_map_.end()) {
    return SlurmxErr::kNonExistentUuid;
  }

  m_resource_avail_ += iter->second;
  m_resource_in_use_ -= iter->second;

  m_resource_uuid_map_.erase(iter);

  return SlurmxErr::kOk;
}

Status SlurmXdServiceImpl::GrantResourceToken(
    ServerContext *context, const GrantResourceTokenRequest *request,
    GrantResourceTokenReply *response) {
  uuid resource_uuid;
  resource_t required_resource;
  SlurmxErr err;

  std::copy(request->resource_uuid().begin(), request->resource_uuid().end(),
            resource_uuid.data);

  required_resource.cpu_count = request->allocated_resource().cpu_core_limit();
  required_resource.memory_bytes =
      request->allocated_resource().memory_limit_bytes();
  required_resource.memory_sw_bytes =
      request->allocated_resource().memory_sw_limit_bytes();

  err = g_server->GrantResourceToken(resource_uuid, required_resource);
  if (err == SlurmxErr::kOk)
    response->set_ok(true);
  else {
    response->set_ok(false);
    response->set_reason(std::string(SlurmxErrStr(err)));
  }

  return Status::OK;
}

Status SlurmXdServiceImpl::RevokeResourceToken(
    ServerContext *context, const RevokeResourceTokenRequest *request,
    RevokeResourceTokenReply *response) {
  uuid resource_uuid;
  SlurmxErr err;

  std::copy(request->resource_uuid().begin(), request->resource_uuid().end(),
            resource_uuid.data);

  err = g_server->RevokeResourceToken(resource_uuid);
  if (err == SlurmxErr::kOk)
    response->set_ok(true);
  else {
    response->set_ok(false);
    response->set_reason(std::string(SlurmxErrStr(err)));
  }

  return Status::OK;
}

SlurmxErr XdServer::CheckValidityOfResourceUuid(const uuid &resource_uuid) {
  std::lock_guard<std::mutex> lock_guard(m_node_resource_mtx_);

  size_t count = m_resource_uuid_map_.count(resource_uuid);
  if (count > 0) return SlurmxErr::kOk;

  return SlurmxErr::kNonExistentUuid;
}

std::optional<resource_t> XdServer::FindResourceByUuid(
    const uuid &resource_uuid) {
  std::lock_guard<std::mutex> lock_guard(m_node_resource_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter != m_resource_uuid_map_.end()) return iter->second;

  return std::nullopt;
}

}  // namespace Xd
