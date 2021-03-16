#include "SrunXClient.h"

SlurmxErr SrunXClient::Init(std::string Xdserver_addr_port,
                            std::string CtlXdserver_addr_port) {
  channel = grpc::CreateChannel(Xdserver_addr_port,
                                grpc::InsecureChannelCredentials());
  channel_ctld = grpc::CreateChannel(CtlXdserver_addr_port,
                                     grpc::InsecureChannelCredentials());
  using namespace std::chrono_literals;
  bool ok;
  ok = channel->WaitForConnected(std::chrono::system_clock::now() + 3s) ||
       channel_ctld->WaitForConnected(std::chrono::system_clock::now() + 3s);
  if (!ok) {
    return SlurmxErr::kConnectionTimeout;
  }
  m_stub_ = SlurmXd::NewStub(channel);
  m_stub_ctld_ = SlurmCtlXd::NewStub(channel_ctld);

  m_stream_ = m_stub_->SrunXStream(&m_context_);
  return SlurmxErr::kOk;
}

SlurmxErr SrunXClient::Run() {
  uuid resource_uuid;
  err = SlurmxErr::kOk;
  state = SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD;
  ClientContext context;
  Status status;
  ResourceAllocReply resourceAllocReply;
  while (true) {
    switch (state) {
      case SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD: {
        ResourceAllocRequest resourceAllocRequest;
        slurmx_grpc::AllocatableResource* AllocatableResource =
            resourceAllocRequest.mutable_required_resource();
        slurmx_grpc::AllocatableResource allocatableResource;
        allocatableResource.set_cpu_core_limit(
            this->allocatableResource.cpu_core_limit);
        allocatableResource.set_memory_sw_limit_bytes(
            this->allocatableResource.memory_sw_limit_bytes);
        allocatableResource.set_memory_limit_bytes(
            this->allocatableResource.memory_limit_bytes);
        AllocatableResource->CopyFrom(allocatableResource);

        status = m_stub_ctld_->AllocateResource(&context, resourceAllocRequest,
                                                &resourceAllocReply);

        if (status.ok()) {
          if (resourceAllocReply.ok()) {
            std::copy(resourceAllocReply.resource_uuid().begin(),
                      resourceAllocReply.resource_uuid().end(),
                      resource_uuid.data);
            SLURMX_DEBUG("Srunxclient: Get the token {} from the slurmxctld.",
                         to_string(resource_uuid));
            this->taskinfo.resource_uuid = resource_uuid;
            state = SrunX_State::NEGOTIATION_TO_SLURMXD;
          } else {
            SLURMX_INFO("Error ! Can not get token for reason: {}",
                        resourceAllocReply.reason());
            err = SlurmxErr::kNoTokenReply;
            state = SrunX_State::ABORT;
          }
        } else {
          SLURMX_INFO("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                      status.error_message());
          err = SlurmxErr::kRpcFailed;
          state = SrunX_State::ABORT;
        }
      } break;

      case SrunX_State::NEGOTIATION_TO_SLURMXD: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::Negotiation);

        slurmx_grpc::Negotiation* negotiation = request.mutable_negotiation();
        slurmx_grpc::Negotiation nego;
        nego.set_version(kVersion);
        negotiation->CopyFrom(nego);

        m_stream_->Write(request);
        state = SrunX_State::NEWTASK_TO_SLURMXD;
      } break;

      case SrunX_State::NEWTASK_TO_SLURMXD: {
        SrunXStreamRequest request;
        request.set_type(SrunXStreamRequest::NewTask);

        slurmx_grpc::TaskInfo* taskInfo = request.mutable_task_info();
        slurmx_grpc::TaskInfo taskinfo;
        std::string str = this->taskinfo.executive_path;
        taskinfo.set_executive_path(str);

        for (std::string arg : this->taskinfo.arguments) {
          taskinfo.add_arguments(arg);
        }

        taskinfo.set_resource_uuid(this->taskinfo.resource_uuid.data,
                                   resource_uuid.size());
        taskInfo->CopyFrom(taskinfo);

        m_stream_->Write(request);
        state = SrunX_State::WAIT_FOR_REPLY_OR_SEND_SIG;
        break;
      }

      case SrunX_State::WAIT_FOR_REPLY_OR_SEND_SIG: {
        SLURMX_INFO("wait");
        signal(SIGINT, SendSignal);
        SrunXStreamReply reply;
//        m_client_wait_thread_ =
//            std::thread(&SrunXClient::m_client_wait_func_, this);
//        signal(SIGINT, ModifySignalFlag);
//        m_client_wait_thread_.detach();
        bool ok;
        ok = m_stream_->Read(&reply);
        SLURMX_INFO("m_stream_ ok is {}",ok);
        if (ok) {
          SLURMX_INFO("reply type is : {}",reply.type());
          if (reply.type() == SrunXStreamReply::IoRedirection) {
            SLURMX_INFO("reply_buf:{}", reply.io_redirection().buf());
//            reply.clear_payload();
          } else if (reply.type() == slurmx_grpc::SrunXStreamReply_Type_ExitStatus) {
            SLURMX_INFO("Srunxclient: Slurmxd exit for signal {}.",
                        reply.task_exit_status().reason());
            SLURMX_INFO("12312");
            state = SrunX_State::FINISH;
          } else if(reply.type()==slurmx_grpc::SrunXStreamReply_Type_NewTaskResult){
            if (reply.new_task_result().ok()) {
              SLURMX_INFO("New Task Success!");
            } else {
              SLURMX_INFO("New Task Failed, For the reason {} .",
                          reply.new_task_result().reason());
              err = SlurmxErr::kNewTaskFailed;
            }
          }
        } else{
          SLURMX_INFO("stream broken!");
          state = SrunX_State::FINISH;
        }
      } break;

      case SrunX_State::ABORT:
        m_stream_->WritesDone();
        status = m_stream_->Finish();
        SLURMX_DEBUG("Connection from peer {} aborted.", context.peer());
        return err;

      case SrunX_State::FINISH:
        SLURMX_INFO("finish");
        m_stream_->WritesDone();
        status = m_stream_->Finish();
        return err;
    }
  }
}

void SrunXClient::SendSignal(int signo) {
  SrunXStreamRequest request;
  SLURMX_INFO("Ctrl C");
  request.set_type(SrunXStreamRequest::Signal);
  request.set_signum(2);
  m_stream_->Write(request);
}

//void SrunXClient::ModifySignalFlag(int signo) {
//  SLURMX_INFO("Srunxclient: Press down 'Ctrl+C'");
//  std::unique_lock<std::mutex> lk(m_cv_m_);
//  m_fg_=1;
//  m_cv_.notify_all();
//}
//
//void SrunXClient::m_client_wait_func_() {
//  std::unique_lock<std::mutex> lk(m_cv_m_);
//  m_cv_.wait(lk,[]{return m_fg_==1;});
//  SrunXStreamRequest request;
//  request.set_type(SrunXStreamRequest::Signal);
//  request.set_signum(2);
//  m_stream_->Write(request);
//}