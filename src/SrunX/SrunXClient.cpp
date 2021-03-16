#include "SrunXClient.h"

SlurmxErr SrunXClient::Init(std::string Xdserver_addr_port,
                            std::string CtlXdserver_addr_port) {
  // catch Ctrl+C signal and send the signal to slurmxd server
  signal(SIGINT, m_modify_signal_flag_);
  m_channel_ = grpc::CreateChannel(Xdserver_addr_port,
                                   grpc::InsecureChannelCredentials());
  m_channel_ctld_ = grpc::CreateChannel(CtlXdserver_addr_port,
                                        grpc::InsecureChannelCredentials());
  using namespace std::chrono_literals;
  bool ok;
  ok = m_channel_->WaitForConnected(std::chrono::system_clock::now() + 3s) ||
       m_channel_ctld_->WaitForConnected(std::chrono::system_clock::now() + 3s);
  if (!ok) {
    return SlurmxErr::kConnectionTimeout;
  }
  m_stub_ = SlurmXd::NewStub(m_channel_);
  m_stub_ctld_ = SlurmCtlXd::NewStub(m_channel_ctld_);

  m_stream_ = m_stub_->SrunXStream(&m_context_);
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
            SLURMX_DEBUG("Get the token {} from the slurmxctld.",
                         to_string(resource_uuid));
            this->taskinfo.resource_uuid = resource_uuid;
            m_state_ = SrunX_State::NEGOTIATION_TO_SLURMXD;
          } else {
            SLURMX_ERROR("Can not get token for reason: {}",
                         resourceallocreply.reason());
            m_err_ = SlurmxErr::kNoTokenReply;
            m_state_ = SrunX_State::ABORT;
          }
        } else {
          SLURMX_ERROR("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                       status.error_message());
          m_err_ = SlurmxErr::kRpcFailed;
          m_state_ = SrunX_State::ABORT;
        }
      } break;

      case SrunX_State::NEGOTIATION_TO_SLURMXD: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::Negotiation);

        auto *result = request.mutable_negotiation();
        result->set_version(kVersion);

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
        // create a thread to listening Ctrl+C signal
        m_client_wait_thread_ =
            std::thread(&SrunXClient::m_client_wait_func_, this);
        m_client_wait_thread_.detach();
        bool ok;
        ok = m_stream_->Read(&reply);
        if (ok) {
          if (reply.type() == SrunXStreamReply::IoRedirection) {
            fmt::print("{}", reply.io_redirection().buf());
          } else if (reply.type() ==
                     slurmx_grpc::SrunXStreamReply_Type_ExitStatus) {
            if(reply.task_exit_status().reason()==slurmx_grpc::TaskExitStatus::Signal){
              SLURMX_DEBUG("Task terminated with signum : {}",
                           reply.task_exit_status().value());
            } else{
              SLURMX_DEBUG("Task finished with exit code: {}",
                           reply.task_exit_status().value());
            }
            //notify wait thread, prevent from main thread block.
            std::unique_lock<std::mutex> lk(m_cv_m_);
            m_exit_fg_ = 1;
            m_cv_.notify_all();
            m_state_ = SrunX_State::FINISH;

          } else if (reply.type() ==
                     slurmx_grpc::SrunXStreamReply_Type_NewTaskResult) {
            if (reply.new_task_result().ok()) {
              SLURMX_DEBUG("New Task Success!");
            } else {
              SLURMX_ERROR("New Task Failed, For the reason {} .",
                           reply.new_task_result().reason());
              m_err_ = SlurmxErr::kNewTaskFailed;
            }
          }
        } else {
          SLURMX_ERROR("Stream broke!");
          m_state_ = SrunX_State::FINISH;
        }
      } break;

      case SrunX_State::ABORT:
        SLURMX_ERROR("Connection to peer {} aborted.", context.peer());
        return m_err_;

      case SrunX_State::FINISH:
        return m_err_;
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
  //The wait will be notified either by signal or exitstatus,
  //but only send msg to slurmXd when notified by signal.
  m_cv_.wait(lk, [] { return (m_signal_fg_ == 1 || m_exit_fg_ == 1); });
  if (m_signal_fg_ == 1) {
    SrunXStreamRequest request;
    request.set_type(SrunXStreamRequest::Signal);
    request.set_signum(2);
    m_stream_->Write(request);
  }
}

void SrunXClient::Wait() {
  m_stream_->WritesDone();
  m_stream_->Finish();
}
