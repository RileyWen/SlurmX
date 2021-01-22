#include "SrunXClient.h"
namespace SrunX {

SlurmxErr SrunXClient::Init(std::string Xdserver_addr_port,
                            std::string CtlXdserver_addr_port) {
  // catch SIGINT signal and call m_modify_signal_flag_ function
  signal(SIGINT, m_modify_signal_flag_);
  // create a thread to listen SIGINT signal
  m_client_wait_thread_ = std::thread(&SrunXClient::m_client_wait_func_, this);
  m_channel_ = grpc::CreateChannel(Xdserver_addr_port,
                                   grpc::InsecureChannelCredentials());
  m_channel_ctld_ = grpc::CreateChannel(CtlXdserver_addr_port,
                                        grpc::InsecureChannelCredentials());
  using namespace std::chrono_literals;
  bool ok = false;
  if (m_channel_->WaitForConnected(std::chrono::system_clock::now() + 3s) &&
      m_channel_ctld_->WaitForConnected(std::chrono::system_clock::now() +
                                        3s)) {
    ok = true;
  }
  m_stub_ = SlurmXd::NewStub(m_channel_);
  m_stub_ctld_ = SlurmCtlXd::NewStub(m_channel_ctld_);

  m_stream_ = m_stub_->SrunXStream(&m_context_);
  if (!ok) {
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_exit_fg_ = 1;
    m_cv_.notify_all();
    return SlurmxErr::kConnectionTimeout;
  }

  return SlurmxErr::kOk;
}

SlurmxErr SrunXClient::Run() {
  uuid resource_uuid;
  m_err_ = SlurmxErr::kOk;
  m_state_ = SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD;
  ClientContext context;
  Status status;
  ResourceAllocReply resourceallocreply;
  while (true) {
    switch (m_state_) {
      case SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD: {
        ResourceAllocRequest resourceallocrequest;
        auto *AllocatableResource =
            resourceallocrequest.mutable_required_resource();
        AllocatableResource->set_cpu_core_limit(
            this->allocatableResource.cpu_core_limit);
        AllocatableResource->set_memory_sw_limit_bytes(
            this->allocatableResource.memory_sw_limit_bytes);
        AllocatableResource->set_memory_limit_bytes(
            this->allocatableResource.memory_limit_bytes);
        status = m_stub_ctld_->AllocateResource(&context, resourceallocrequest,
                                                &resourceallocreply);

        if (status.ok()) {
          if (resourceallocreply.ok()) {
            std::copy(resourceallocreply.resource_uuid().begin(),
                      resourceallocreply.resource_uuid().end(),
                      resource_uuid.data);
            SLURMX_TRACE("Get the token {} from the slurmxctld.",
                         to_string(resource_uuid));
            this->taskinfo.resource_uuid = resource_uuid;
            m_state_ = SrunX_State::NEGOTIATION_TO_SLURMXD;
          } else {
            SLURMX_DEBUG("Can not get token for reason: {}",
                         resourceallocreply.reason());
            m_err_ = SlurmxErr::kNoTokenReply;
            m_state_ = SrunX_State::ABORT;
          }
        } else {
          SLURMX_DEBUG("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                       status.error_message());
          m_err_ = SlurmxErr::kRpcFailed;
          m_state_ = SrunX_State::ABORT;
        }
      } break;

      case SrunX_State::NEGOTIATION_TO_SLURMXD: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::Negotiation);

        auto *result = request.mutable_negotiation();
        result->set_version(SrunX::kVersion);

        m_stream_->Write(request);
        m_state_ = SrunX_State::NEWTASK_TO_SLURMXD;
      } break;

      case SrunX_State::NEWTASK_TO_SLURMXD: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::NewTask);

        auto *result = request.mutable_task_info();
        std::string str = this->taskinfo.executive_path;
        result->set_executive_path(str);

        for (std::string arg : this->taskinfo.arguments) {
          result->add_arguments(arg);
        }

        result->set_resource_uuid(this->taskinfo.resource_uuid.data,
                                  resource_uuid.size());

        m_stream_->Write(request);
        m_state_ = SrunX_State::WAIT_FOR_REPLY_OR_SEND_SIG;
        break;
      }

      case SrunX_State::WAIT_FOR_REPLY_OR_SEND_SIG: {
        SrunXStreamReply reply;
        bool ok;
        ok = m_stream_->Read(&reply);
        if (ok) {
          if (reply.type() == SrunXStreamReply::IoRedirection) {
            fmt::print("{}\n", reply.io_redirection().buf());
          } else if (reply.type() ==
                     slurmx_grpc::SrunXStreamReply_Type_ExitStatus) {
            if (reply.task_exit_status().reason() ==
                slurmx_grpc::TaskExitStatus::Signal) {
              fmt::print("The task was terminated with signal {}.",
                         strsignal(reply.task_exit_status().value()));
            } else {
              SLURMX_DEBUG("Task finished with exit code: {}",
                           reply.task_exit_status().value());
            }
            m_state_ = SrunX_State::FINISH;

          } else if (reply.type() ==
                     slurmx_grpc::SrunXStreamReply_Type_NewTaskResult) {
            if (reply.new_task_result().ok()) {
              SLURMX_DEBUG("The new task starts runing...");
            } else {
              SLURMX_DEBUG("Failed to create the new task. Reason: {}.",
                           reply.new_task_result().reason());
              m_err_ = SlurmxErr::kNewTaskFailed;
            }
          }
        } else {
          SLURMX_DEBUG("Stream is broken!");
          m_err_ = SlurmxErr::KStreamBroken;
          m_state_ = SrunX_State::FINISH;
        }
      } break;

      case SrunX_State::ABORT: {
        std::unique_lock<std::mutex> lk(m_cv_m_);
        // notify wait thread, prevent main thread blocking.
        m_exit_fg_ = 1;
        m_cv_.notify_all();
        SLURMX_DEBUG("Connection to peer {} aborted.", context.peer());
        return m_err_;
      }

      case SrunX_State::FINISH: {
        // notify wait thread, prevent main thread blocking.
        std::unique_lock<std::mutex> lk(m_cv_m_);
        m_exit_fg_ = 1;
        m_cv_.notify_all();
        return m_err_;
      }
    }
  }
}

void SrunXClient::m_modify_signal_flag_(int signo) {
  SLURMX_TRACE("Press down 'Ctrl+C'");
  std::unique_lock<std::mutex> lk(m_cv_m_);
  m_signal_fg_ = 1;
  m_cv_.notify_all();
}

void SrunXClient::m_client_wait_func_() {
  std::unique_lock<std::mutex> lk(m_cv_m_);
  // The wait will be notified by signal or exitstatus,
  // but only send msg to slurmXd when notified by signal.
  m_cv_.wait(lk, [] { return (m_signal_fg_ == 1 || m_exit_fg_ == 1); });
  if (m_signal_fg_ == 1) {
    SrunXStreamRequest request;
    request.set_type(SrunXStreamRequest::Signal);
    request.set_signum(2);
    m_stream_->Write(request);
  }
}

void SrunXClient::Wait() {
  m_client_wait_thread_.join();
  m_stream_->WritesDone();
  m_stream_->Finish();
}
}  // namespace SrunX