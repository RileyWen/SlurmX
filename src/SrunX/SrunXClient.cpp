#include "SrunXClient.h"

namespace SrunX {

SrunXClient::~SrunXClient() {
  m_is_ending_ = true;
  Wait();
}

SlurmxErr SrunXClient::Init(std::string xd_addr_port,
                            std::string ctlxd_addr_port) {
  using namespace std::chrono_literals;

  signal(SIGINT, SigintHandlerFunc_);
  m_sigint_grpc_send_thread_ =
      std::thread(&SrunXClient::SigintGrpcSendThreadFunc_, this);

  m_xd_channel_ =
      grpc::CreateChannel(xd_addr_port, grpc::InsecureChannelCredentials());
  m_ctld_channel_ =
      grpc::CreateChannel(ctlxd_addr_port, grpc::InsecureChannelCredentials());

  bool ok;

  ok = m_xd_channel_->WaitForConnected(std::chrono::system_clock::now() + 3s);
  if (!ok) {
    SLURMX_ERROR("Cannot connect to Xd server!");
    return SlurmxErr::kConnectionTimeout;
  }

  ok = m_ctld_channel_->WaitForConnected(std::chrono::system_clock::now() + 3s);
  if (!ok) {
    SLURMX_ERROR("Cannot connect to ctlXd server!");
    return SlurmxErr::kConnectionTimeout;
  }

  m_xd_stub_ = SlurmXd::NewStub(m_xd_channel_);
  m_ctld_stub_ = SlurmCtlXd::NewStub(m_ctld_channel_);

  return SlurmxErr::kOk;
}

SlurmxErr SrunXClient::Run(const CommandLineArgs &cmd_args) {
  uuid resource_uuid;
  SlurmxErr err;

  err = RequestResourceToken_(cmd_args, &resource_uuid);
  if (err != SlurmxErr::kOk) {
    m_is_ending_ = true;
    return err;
  }

  err = EstablishSrunXStream_(cmd_args, resource_uuid);
  m_is_ending_ = true;

  return err;
}

void SrunXClient::SigintHandlerFunc_(int) {
  static bool ctrl_c_pressed{false};

  if (ctrl_c_pressed) {
    SLURMX_TRACE("Ctrl+C was already caught. Ignoring it.");
  } else {
    SLURMX_TRACE("Ctrl+C is caught.");
    ctrl_c_pressed = true;
    s_sigint_received_.store(true, std::memory_order_release);
  }
}

void SrunXClient::SigintGrpcSendThreadFunc_() {
  // The cv will be notified when SIGINT is caught or m_is_ending_ == true.
  while (true) {
    if (s_sigint_received_.load(std::memory_order_relaxed)) {
      if (m_stream_) {
        // Send the signal to SlurmXd. Then the state machine will move
        // forward.
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::Signal);
        request.set_signum(2);
        m_stream_->Write(request);
      }

      break;
    }

    if (m_is_ending_.load(std::memory_order_relaxed)) break;

    std::this_thread::yield();
  }
}

void SrunXClient::Wait() {
  bool expected = false;
  bool was_exchanged;

  was_exchanged =
      m_is_under_destruction_.compare_exchange_strong(expected, true);
  if (was_exchanged) {
    // clean up ONLY once
    if (m_sigint_grpc_send_thread_.joinable())
      m_sigint_grpc_send_thread_.join();
    if (m_stream_) {
      SrunXStreamReply reply;

      // m_stream_->Finish() will block forever if m_stream_ has unread trailing
      // messages. Use this while loop to make sure of no trailing messages.
      //
      // Note: The state machine of SrunX is revised to guarantee no trailing
      // messages.
      //  Normally, the Read() should always return false.
      while (m_stream_->Read(&reply))
        SLURMX_TRACE("Reading trailing replies: type: {}", reply.type());
      m_stream_->Finish();
    }
  }
}

SlurmxErr SrunXClient::RequestResourceToken_(const CommandLineArgs &cmd_args,
                                             uuid *resource_uuid) {
  ClientContext alloc_context;
  ResourceAllocRequest alloc_req;
  ResourceAllocReply alloc_reply;

  Status status;

  auto *alloc_res = alloc_req.mutable_required_resource();
  alloc_res->set_cpu_core_limit(cmd_args.required_resource.cpu_count);
  alloc_res->set_memory_limit_bytes(cmd_args.required_resource.memory_bytes);
  alloc_res->set_memory_sw_limit_bytes(
      cmd_args.required_resource.memory_sw_bytes);

  status =
      m_ctld_stub_->AllocateResource(&alloc_context, alloc_req, &alloc_reply);

  if (status.ok()) {
    if (alloc_reply.ok()) {
      std::copy(alloc_reply.resource_uuid().begin(),
                alloc_reply.resource_uuid().end(), resource_uuid->data);
      SLURMX_TRACE("Resource allocated from CtlXd. UUID: {}",
                   to_string(*resource_uuid));
      return SlurmxErr::kOk;
    } else {
      SLURMX_ERROR("Failed to allocate required resource from CtlXd: {}",
                   alloc_reply.reason());
      return SlurmxErr::kTokenRequestFailure;
    }
  } else {
    SLURMX_DEBUG("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                 status.error_message());
    return SlurmxErr::kRpcFailure;
  }
}

SlurmxErr SrunXClient::EstablishSrunXStream_(const CommandLineArgs &cmd_args,
                                             const uuid &resource_uuid) {
  m_stream_ = m_xd_stub_->SrunXStream(&m_stream_context_);

  SlurmxErr err = SlurmxErr::kOk;

  enum class SrunxState {
    kNegotiationWithSlurmxd = 0,
    kRequestNewTaskFromSlurmxd,
    kWaitForNewTaskReply,
    kWaitForIoRedirectionOrSignal,
    kAbort,
    kFinish,
  };

  SrunxState state = SrunxState::kNegotiationWithSlurmxd;
  Status status;
  while (true) {
    switch (state) {
      case SrunxState::kNegotiationWithSlurmxd: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::Negotiation);

        auto *result = request.mutable_negotiation();
        result->set_version(SrunX::kSrunVersion);

        m_stream_->Write(request);
        state = SrunxState::kRequestNewTaskFromSlurmxd;
      } break;

      case SrunxState::kRequestNewTaskFromSlurmxd: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::NewTask);

        auto *result = request.mutable_task_info();
        std::string str = cmd_args.executive_path;
        result->set_executive_path(str);

        for (std::string arg : cmd_args.arguments) {
          result->add_arguments(arg);
        }

        result->set_resource_uuid(resource_uuid.data, resource_uuid.size());

        m_stream_->Write(request);
        state = SrunxState::kWaitForNewTaskReply;
        break;
      }

      case SrunxState::kWaitForNewTaskReply: {
        SrunXStreamReply reply;

        if (m_stream_->Read(&reply)) {
          if (reply.type() == SrunXStreamReply::NewTaskResult) {
            if (reply.new_task_result().ok()) {
              SLURMX_TRACE("The new task starts running...");
              state = SrunxState::kWaitForIoRedirectionOrSignal;
            } else {
              SLURMX_DEBUG("Failed to create the new task. Reason: {}.",
                           reply.new_task_result().reason());
              err = SlurmxErr::kRpcFailure;
              state = SrunxState::kFinish;
            }
          }
        } else {
          SLURMX_DEBUG("Stream is broken while waiting for new task result!");
          err = SlurmxErr::KStreamBroken;
          state = SrunxState::kAbort;
        }

        break;
      }

      case SrunxState::kWaitForIoRedirectionOrSignal: {
        SrunXStreamReply reply;

        if (m_stream_->Read(&reply)) {
          if (reply.type() == SrunXStreamReply::IoRedirection) {
            fmt::print("{}", reply.io_redirection().buf());

          } else if (reply.type() == SrunXStreamReply::ExitStatus) {
            if (reply.task_exit_status().reason() == TaskExitStatus::Signal) {
              fmt::print("The task was terminated with signal: {}.\n",
                         strsignal(reply.task_exit_status().value()));
            } else {
              SLURMX_DEBUG("Task exit normally with value: {}",
                           reply.task_exit_status().value());
            }
            state = SrunxState::kFinish;

          } else {
            SLURMX_DEBUG(
                "Stream is broken when waiting for I/O, signal or exit "
                "status!");
            err = SlurmxErr::KStreamBroken;
            state = SrunxState::kFinish;
          }
        }
        break;
      }

      case SrunxState::kAbort: {
        SLURMX_DEBUG("Connection to peer {} aborted.",
                     m_stream_context_.peer());

        m_stream_->WritesDone();
        return err;
      }

      case SrunxState::kFinish: {
        SLURMX_DEBUG("Connection to peer {} finished.",
                     m_stream_context_.peer());

        m_stream_->WritesDone();
        return err;
      }
    }
  }
}

}  // namespace SrunX