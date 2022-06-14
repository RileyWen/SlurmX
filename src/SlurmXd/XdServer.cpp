#include "XdServer.h"

#include <arpa/inet.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <boost/algorithm/string/join.hpp>
#include <filesystem>
#include <fstream>
#include <utility>

#include "CtlXdClient.h"

namespace Xd {

using boost::uuids::uuid;

Status SlurmXdServiceImpl::SrunXStream(
    ServerContext *context,
    ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest> *stream) {
  SLURMX_DEBUG("SrunX connects from {}", context->peer());

  enum class StreamState {
    kNegotiation = 0,
    kCheckResource,
    kExecutiveInfo,
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

  // A task id is bound to one connection.
  uint32_t task_id;
  // A resource uuid is bound to one task.
  uuid resource_uuid;

  StreamState state = StreamState::kNegotiation;
  while (true) {
    switch (state) {
      case StreamState::kNegotiation:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::NegotiationType) {
            SLURMX_DEBUG("Expect negotiation from peer {}, but none.",
                         context->peer());
            state = StreamState::kAbort;
          } else {
            SLURMX_DEBUG("Negotiation from peer: {}", context->peer());
            reply.Clear();
            reply.set_type(SrunXStreamReply::ResultType);
            reply.mutable_result()->set_ok(true);

            stream_w_mtx.lock();
            stream->Write(reply);
            stream_w_mtx.unlock();

            state = StreamState::kCheckResource;
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }
        break;

      case StreamState::kCheckResource:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::CheckResourceType) {
            SLURMX_DEBUG("Expect CheckResource from peer {}, but got {}.",
                         context->peer(), request.GetTypeName());
            state = StreamState::kAbort;
          } else {
            std::copy(request.check_resource().resource_uuid().begin(),
                      request.check_resource().resource_uuid().end(),
                      resource_uuid.data);

            task_id = request.check_resource().task_id();
            // Check the validity of resource uuid provided by client.
            err = g_server->CheckValidityOfResourceUuid(resource_uuid, task_id);
            if (err != SlurmxErr::kOk) {
              // The resource uuid provided by Client is invalid. Reject.
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);

              auto *result = reply.mutable_result();
              result->set_ok(false);
              result->set_reason(
                  fmt::format("Resource uuid invalid: {}",
                              (err == SlurmxErr::kNonExistent
                                   ? "Not Existent"
                                   : "It doesn't match with task_id")));

              stream_w_mtx.lock();
              stream->Write(reply, grpc::WriteOptions());
              stream_w_mtx.unlock();

              state = StreamState::kFinish;
            } else {
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);
              reply.mutable_result()->set_ok(true);

              stream_w_mtx.lock();
              stream->Write(reply);
              stream_w_mtx.unlock();

              state = StreamState::kExecutiveInfo;
            }
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }

        break;
      case StreamState::kExecutiveInfo:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::ExecutiveInfoType) {
            SLURMX_DEBUG("Expect CheckResource from peer {}, but got {}.",
                         context->peer(), request.GetTypeName());
            state = StreamState::kAbort;
          } else {
            std::forward_list<std::string> arguments;
            auto iter = arguments.before_begin();
            for (const auto &arg : request.exec_info().arguments()) {
              iter = arguments.emplace_after(iter, arg);
            }

            // We have checked the validity of resource uuid. Now execute it.

            // It's safe to call stream->Write() on a closed stream.
            // (stream->Write() just return false rather than throwing an
            // exception).
            auto output_callback = [stream, &write_mtx = stream_w_mtx](
                                       std::string &&buf, void *user_data) {
              SLURMX_TRACE("Output Callback called. buf: {}", buf);
              SlurmxGrpc::SrunXStreamReply reply;
              reply.set_type(SrunXStreamReply::IoRedirectionType);

              std::string *reply_buf = reply.mutable_io()->mutable_buf();
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
                                       bool is_terminated_by_signal, int value,
                                       void *user_data) {
              SLURMX_TRACE("Finish Callback called. signaled: {}, value: {}",
                           is_terminated_by_signal, value);
              SlurmxGrpc::SrunXStreamReply reply;
              reply.set_type(SrunXStreamReply::ExitStatusType);

              SlurmxGrpc::StreamReplyExitStatus *stat =
                  reply.mutable_exit_status();
              stat->set_reason(is_terminated_by_signal
                                   ? SlurmxGrpc::StreamReplyExitStatus::Signal
                                   : SlurmxGrpc::StreamReplyExitStatus::Normal);
              stat->set_value(value);

              // stream->WriteLast() shall not be used here.
              // On the server side, WriteLast cause all the Write() to be
              // blocked until the service handler returned.
              // WriteLast() should actually be called on the client side.
              write_mtx.lock();
              stream->Write(reply, grpc::WriteOptions());
              write_mtx.unlock();

              // If this line is appended, when SrunX has no response to
              // WriteLast, the connection can stop anyway. Otherwise, the
              // connection will stop (i.e. stream->Read() returns false) only
              // if 1. SrunX calls stream->WriteLast() or 2. the underlying
              // channel is broken. However, the 2 situations cover all
              // situations that we can meet, so the following line should
              // not be added except when debugging.
              //
              // context->TryCancel();
            };

            std::list<std::string> args;
            for (auto &&arg : request.exec_info().arguments())
              args.push_back(arg);

            err = g_task_mgr->SpawnInteractiveTaskAsync(
                task_id, request.exec_info().executive_path(), std::move(args),
                std::move(output_callback), std::move(finish_callback));
            if (err == SlurmxErr::kOk) {
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);

              auto *result = reply.mutable_result();
              result->set_ok(true);

              stream_w_mtx.lock();
              stream->Write(reply);
              stream_w_mtx.unlock();

              state = StreamState::kWaitForEofOrSigOrTaskEnd;
            } else {
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);

              auto *result = reply.mutable_result();
              result->set_ok(false);

              if (err == SlurmxErr::kSystemErr)
                result->set_reason(
                    fmt::format("System error: {}", strerror(errno)));
              else if (err == SlurmxErr::kStop)
                result->set_reason("Server is stopping");
              else
                result->set_reason(fmt::format("Unknown failure. Code: . ",
                                               uint16_t(err),
                                               SlurmxErrStr(err)));

              stream_w_mtx.lock();
              stream->Write(reply);
              stream_w_mtx.unlock();

              state = StreamState::kFinish;
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
          if (request.type() != SrunXStreamRequest::SignalType) {
            SLURMX_DEBUG("Expect signal from peer {}, but none.",
                         context->peer());
            state = StreamState::kAbort;
          } else {
            // If ctrl+C is pressed before the task ends, inform TaskManager
            // of the interrupt and wait for TaskManager to stop the Task.

            SLURMX_TRACE("Receive signum {} from client. Killing task {}",
                         request.signum(), task_id);

            // Todo: Sometimes, TaskManager can't kill a task, there're some
            //  problems here.

            g_task_mgr->TerminateTaskAsync(task_id);

            // The state machine does not switch the state here.
            // We just use stream->Read() to wait for the task to end.
            // When the task ends, the finish_callback will shut down the
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

      case StreamState::kAbort: {
        SLURMX_DEBUG("Connection from peer {} aborted.", context->peer());

        // Invalidate resource uuid and free the resource in use.
        g_server->RevokeResourceToken(resource_uuid);

        return Status::CANCELLED;
      }

      case StreamState::kFinish: {
        SLURMX_TRACE("Connection from peer {} finished normally",
                     context->peer());

        // Invalidate resource uuid and free the resource in use.
        g_server->RevokeResourceToken(resource_uuid);

        return Status::OK;
      }

      default:
        SLURMX_ERROR("Unexpected XdServer State: {}", state);
        return Status::CANCELLED;
    }
  }
}

void XdServer::GrantResourceToken(const uuid &resource_uuid, uint32_t task_id) {
  LockGuard guard(m_mtx_);
  m_resource_uuid_map_[resource_uuid] = task_id;
}

SlurmxErr XdServer::RevokeResourceToken(const uuid &resource_uuid) {
  LockGuard guard(m_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter == m_resource_uuid_map_.end()) {
    return SlurmxErr::kNonExistent;
  }

  m_resource_uuid_map_.erase(iter);

  return SlurmxErr::kOk;
}

grpc::Status SlurmXdServiceImpl::ExecuteTask(
    grpc::ServerContext *context, const SlurmxGrpc::ExecuteTaskRequest *request,
    SlurmxGrpc::ExecuteTaskReply *response) {
  SLURMX_TRACE("Received a task with id {}", request->task().task_id());

  g_task_mgr->ExecuteTaskAsync(request->task());

  response->set_ok(true);
  return Status::OK;
}

grpc::Status SlurmXdServiceImpl::TerminateTask(
    grpc::ServerContext *context,
    const SlurmxGrpc::TerminateTaskRequest *request,
    SlurmxGrpc::TerminateTaskReply *response) {
  g_task_mgr->TerminateTaskAsync(request->task_id());

  return Status::OK;
}

grpc::Status SlurmXdServiceImpl::QueryTaskIdFromPort(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryTaskIdFromPortRequest *request,
    SlurmxGrpc::QueryTaskIdFromPortReply *response) {
  SLURMX_TRACE("Receive QueryTaskIdFromPort RPC from {}: port: {}",
               context->peer(), request->port());

  std::string port_hex = fmt::format("{:0>4X}", request->port());

  ino_t inode;

  // find inode
  // 1._find_match_in_tcp_file
  std::string tcp_path{"/proc/net/tcp"};
  std::ifstream tcp_in(tcp_path, std::ios::in);
  std::string tcp_line;
  bool inode_found = false;
  if (tcp_in) {
    getline(tcp_in, tcp_line);  // Skip the header line
    while (getline(tcp_in, tcp_line)) {
      boost::trim(tcp_line);
      std::vector<std::string> tcp_line_vec;
      boost::split(tcp_line_vec, tcp_line, boost::is_any_of(" :"),
                   boost::token_compress_on);
      SLURMX_TRACE("Checking port {} == {}", port_hex, tcp_line_vec[2]);
      if (port_hex == tcp_line_vec[2]) {
        inode_found = true;
        inode = std::stoul(tcp_line_vec[13]);
        SLURMX_TRACE("Inode num for port {} is {}", request->port(), inode);
        break;
      }
    }
    if (!inode_found) {
      SLURMX_TRACE("Inode num for port {} is not found.", request->port());
      response->set_ok(false);
      return Status::OK;
    }
  } else {  // can't find file
    SLURMX_ERROR("Can't open file: {}", tcp_path);
  }

  // 2.find_pid_by_inode
  pid_t pid_i = -1;
  std::filesystem::path proc_path{"/proc"};
  for (auto const &dir_entry : std::filesystem::directory_iterator(proc_path)) {
    if (isdigit(dir_entry.path().filename().string()[0])) {
      std::string pid_s = dir_entry.path().filename().string();
      std::string proc_fd_path =
          fmt::format("{}/{}/fd", proc_path.string(), pid_s);
      if (!std::filesystem::exists(proc_fd_path)) {
        continue;
      }
      for (auto const &fd_dir_entry :
           std::filesystem::directory_iterator(proc_fd_path)) {
        struct stat statbuf {};
        std::string fdpath = fmt::format(
            "{}/{}", proc_fd_path, fd_dir_entry.path().filename().string());
        const char *fdchar = fdpath.c_str();
        if (stat(fdchar, &statbuf) != 0) {
          continue;
        }
        if (statbuf.st_ino == inode) {
          pid_i = std::stoi(pid_s);
          SLURMX_TRACE("Pid for the process that owns port {} is {}",
                       request->port(), pid_i);
          break;
        }
      }
    }
    if (pid_i != -1) {
      break;
    }
  }
  if (pid_i == -1) {
    SLURMX_TRACE("Pid for the process that owns port {} is not found.",
                 request->port());
    response->set_ok(false);
    return Status::OK;
  }

  // 3.slurm_pid2jobid
  do {
    std::optional<uint32_t> task_id_opt =
        g_task_mgr->QueryTaskIdFromPidAsync(pid_i);
    if (task_id_opt.has_value()) {
      SLURMX_TRACE("Task id for pid {} is #{}", pid_i, task_id_opt.value());
      response->set_ok(true);
      response->set_task_id(task_id_opt.value());
      return Status::OK;
    } else {
      std::string proc_dir = fmt::format("/proc/{}/status", pid_i);
      YAML::Node proc_details = YAML::LoadFile(proc_dir);
      if (proc_details["PPid"]) {
        pid_t ppid = std::stoi(proc_details["PPid"].as<std::string>());
        SLURMX_TRACE("Pid {} not found in TaskManager. Checking ppid {}", pid_i,
                     ppid);
        pid_i = ppid;
      } else {
        SLURMX_TRACE(
            "Pid {} not found in TaskManager. "
            "However ppid is 1. Break the loop.",
            pid_i);
        pid_i = 1;
      }
    }
  } while (pid_i > 1);

  response->set_ok(false);
  return Status::OK;
}

grpc::Status SlurmXdServiceImpl::QueryTaskIdFromPortForward(
    grpc::ServerContext *context,
    const SlurmxGrpc::QueryTaskIdFromPortForwardRequest *request,
    SlurmxGrpc::QueryTaskIdFromPortForwardReply *response) {
  SLURMX_TRACE(
      "Receive QueryTaskIdFromPortForward from Pam module: "
      "port: {}, xd_address: {}",
      request->port(), request->target_xd_address());

  std::shared_ptr<Channel> channel_of_remote_xd = grpc::CreateChannel(
      request->target_xd_address(), grpc::InsecureChannelCredentials());

  std::unique_ptr<SlurmxGrpc::SlurmXd::Stub> stub_of_remote_xd =
      SlurmxGrpc::SlurmXd::NewStub(channel_of_remote_xd);

  SlurmxGrpc::QueryTaskIdFromPortRequest request_to_remote_xd;
  SlurmxGrpc::QueryTaskIdFromPortReply reply_from_remote_xd;
  ClientContext context_of_remote_xd;
  Status status_remote_xd;

  request_to_remote_xd.set_port(request->port());

  status_remote_xd = stub_of_remote_xd->QueryTaskIdFromPort(
      &context_of_remote_xd, request_to_remote_xd, &reply_from_remote_xd);
  if (!status_remote_xd.ok()) {
    SLURMX_ERROR("QueryTaskIdFromPort gRPC call failed: {} | {}",
                 status_remote_xd.error_message(),
                 status_remote_xd.error_details());
    response->set_ok(false);
    return Status::OK;
  }

  if (reply_from_remote_xd.ok()) {
    SLURMX_TRACE("ssh client with remote port {} belongs to task #{}",
                 request->port(), reply_from_remote_xd.task_id());
    response->set_ok(true);
    response->set_task_id(reply_from_remote_xd.task_id());
    return Status::OK;
  } else {
    SLURMX_TRACE("ssh client with remote port {} doesn't belong to any task",
                 request->port());
    response->set_ok(false);
    return Status::OK;
  }
}

XdServer::XdServer(std::list<std::string> listen_addresses)
    : m_listen_addresses_(std::move(listen_addresses)) {
  m_service_impl_ = std::make_unique<SlurmXdServiceImpl>();

  grpc::ServerBuilder builder;

  for (auto &&address : m_listen_addresses_) {
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  SLURMX_INFO("SlurmXd is listening on [{}]",
              boost::join(m_listen_addresses_, ", "));

  g_task_mgr->SetSigintCallback([p_server = m_server_.get()] {
    p_server->Shutdown();
    SLURMX_TRACE("Grpc Server Shutdown() was called.");
  });
}

SlurmxErr XdServer::CheckValidityOfResourceUuid(const uuid &resource_uuid,
                                                uint32_t task_id) {
  LockGuard guard(m_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter == m_resource_uuid_map_.end()) return SlurmxErr::kNonExistent;

  if (iter->second != task_id) return SlurmxErr::kInvalidParam;

  return SlurmxErr::kOk;
}

}  // namespace Xd
