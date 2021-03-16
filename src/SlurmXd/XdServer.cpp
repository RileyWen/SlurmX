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

  SlurmxErr err;
  bool ok;
  SrunXStreamRequest request;
  SrunXStreamReply reply;

  // gRPC doesn't support parallel Write() on the same stream.
  // Use mutex to guarantee serial Write() in SrunXStream.
  std::mutex stream_w_mtx;

  // A task name is bound to one connection.
  std::string task_name;

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
            err = g_server->CheckValidityOfResourceUuid(resource_uuid);

            if (err != SlurmxErr::kOk) {
              // The resource uuid provided by Client is invalid. Reject.

              reply.Clear();
              reply.set_type(slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

              auto *result = reply.mutable_new_task_result();
              result->set_ok(false);
              result->set_reason("Resource uuid is invalid");

              stream_w_mtx.lock();
              stream->Write(reply, grpc::WriteOptions());
              stream_w_mtx.unlock();

              state = StreamState::kFinish;
            } else {
              task_name = fmt::format("{}_{}", context->peer(),
                                      g_server->NewTaskSeqNum());

              std::forward_list<std::string> arguments;
              auto iter = arguments.before_begin();
              for (const auto &arg : request.task_info().arguments()) {
                iter = arguments.emplace_after(iter, arg);
              }

              // We have checked the validity of resource uuid.
              std::optional<resource_t> resource_limit =
                  g_server->FindResourceByUuid(resource_uuid);

              Cgroup::CgroupLimit cg_limit{
                  .cpu_core_limit = resource_limit.value().cpu_count,
                  .memory_limit_bytes = resource_limit.value().memory_bytes,
                  .memory_sw_limit_bytes =
                      resource_limit.value().memory_sw_bytes};

              // It's safe to call stream->Write() on a closed stream.
              // (stream->Write() just return false rather than throwing an
              // exception).
              auto output_callback =
                  [stream, &write_mtx = stream_w_mtx](std::string &&buf) {
                    SLURMX_TRACE("Output Callback called. buf: {}", buf);
                    slurmx_grpc::SrunXStreamReply reply;
                    reply.set_type(
                        slurmx_grpc::SrunXStreamReply_Type_IoRedirection);

                    std::string *reply_buf =
                        reply.mutable_io_redirection()->mutable_buf();
                    *reply_buf = std::move(buf);

                    write_mtx.lock();
                    stream->Write(reply);
                    write_mtx.unlock();

                    SLURMX_TRACE("stream->Write() done.");
                  };

              // Call stream->Write() and cause the grpc thread
              // that owns 'stream' to stop the connection handling and quit.
              auto finish_callback = [stream,
                                      &write_mtx = stream_w_mtx /*, context*/](
                                         bool is_terminated_by_signal,
                                         int value) {
                SLURMX_TRACE("Finish Callback called. signaled: {}, value: {}",
                             is_terminated_by_signal, value);
                slurmx_grpc::SrunXStreamReply reply;
                reply.set_type(slurmx_grpc::SrunXStreamReply_Type_ExitStatus);

                slurmx_grpc::TaskExitStatus *stat =
                    reply.mutable_task_exit_status();
                stat->set_reason(
                    is_terminated_by_signal
                        ? slurmx_grpc::TaskExitStatus_ExitReason_Signal
                        : slurmx_grpc::TaskExitStatus_ExitReason_Normal);
                stat->set_value(value);

                // stream->WriteLast() shall not be used here.
                // On the server side, WriteLast cause all the Write() to be
                // blocked until the this service handler returned.
                // WriteLast() should actually be called on the client side.
                write_mtx.lock();
                stream->Write(reply, grpc::WriteOptions());
                write_mtx.unlock();

                // If this line is appended, when SrunX has no response to
                // WriteLast, the connection can stop anyway. Otherwise, the
                // connection will stop (i.e. stream->Read() returns false) only
                // if 1. SrunX calls stream->WriteLast() or 2. the underlying
                // channel is broken. However, the 2 situations is all
                // situations that we will meet, so the following line should
                // not be added except when debugging.
                //
                // context->TryCancel();
              };

              TaskInitInfo task_info{
                  .name = task_name,
                  .executive_path = request.task_info().executive_path(),
                  .arguments = arguments,
                  .cg_limit = cg_limit,
                  .output_callback = std::move(output_callback),
                  .finish_callback = std::move(finish_callback),
              };

              err =
                  TaskManager::GetInstance().AddTaskAsync(std::move(task_info));
              if (err == SlurmxErr::kOk) {
                reply.Clear();
                reply.set_type(
                    slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

                auto *result = reply.mutable_new_task_result();
                result->set_ok(true);

                stream_w_mtx.lock();
                stream->Write(reply);
                stream_w_mtx.unlock();

                state = StreamState::kWaitForEofOrSigOrTaskEnd;
              } else {
                reply.Clear();
                reply.set_type(
                    slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

                auto *result = reply.mutable_new_task_result();
                result->set_ok(false);

                if (err == SlurmxErr::kSystemErr)
                  result->set_reason(
                      fmt::format("System error: {}", strerror(errno)));
                else if (err == SlurmxErr::kStop)
                  result->set_reason("Server is stopping");
                else
                  result->set_reason(
                      fmt::format("Unknown failure. Code: ", uint16_t(err)));

                stream_w_mtx.lock();
                stream->Write(reply);
                stream_w_mtx.unlock();

                state = StreamState::kFinish;
              }
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
            // If ctrl+C is pressed before the task ends, inform TaskManager
            // of the interrupt and wait for TaskManager to stop the Task.

            SLURMX_TRACE("Receive signum {} from client. Killing task {}",
                         request.signum(), task_name);

            err = TaskManager::GetInstance().Kill(task_name, request.signum());
            if (err != SlurmxErr::kOk) {
              SLURMX_ERROR("Failed to kill task {}. Error: {}",
                           SlurmxErrStr(err));
            }

            // The state machine does not switch the state here.
            // We just use stream->Read() to wait for the task to end.
            // When the task ends, the finish_callback will shutdown the
            // stream and cause stream->Read() to return with false.
          }
        } else {
          // If the task ends, the callback which handles the end of a task in
          // TaskManager will send the task end message to client. The client
          // will call stream->Write() to end the stream. Then the
          // stream->Read() returns with ok = false.

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
    return SlurmxErr::kNonExistent;
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

XdServer::XdServer(const std::string &listen_address,
                   const resource_t &total_resource)
    : m_listen_address_(listen_address),
      m_resource_total_(total_resource),
      m_resource_avail_(total_resource),
      m_resource_in_use_() {
  m_service_impl_ = std::make_unique<SlurmXdServiceImpl>();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(m_listen_address_,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  SLURMX_INFO("SlurmXd is listening on {}", m_listen_address_);

  TaskManager::GetInstance().SetSigintCallback([p_server = m_server_.get()] {
    p_server->Shutdown();
    SLURMX_TRACE("Grpc Server Shutdown() was called.");
  });
}

SlurmxErr XdServer::CheckValidityOfResourceUuid(const uuid &resource_uuid) {
  std::lock_guard<std::mutex> lock_guard(m_node_resource_mtx_);

  size_t count = m_resource_uuid_map_.count(resource_uuid);
  if (count > 0) return SlurmxErr::kOk;

  return SlurmxErr::kNonExistent;
}

std::optional<resource_t> XdServer::FindResourceByUuid(
    const uuid &resource_uuid) {
  std::lock_guard<std::mutex> lock_guard(m_node_resource_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter != m_resource_uuid_map_.end()) return iter->second;

  return std::nullopt;
}

}  // namespace Xd
