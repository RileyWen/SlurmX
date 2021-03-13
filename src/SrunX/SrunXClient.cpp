#include "SrunXClient.h"

SlurmxErr SrunXClient::Init(int argc, char* argv[]) {
  SlurmxErr err_parse;
  err_parse = parser.Parse(argc, argv);
  if (err_parse != SlurmxErr::kOk) {
    return err_parse;
  }
  parser.GetTaskInfo(this->taskinfo);
  parser.GetAllocatableResource(this->allocatableResource);

  channel = grpc::CreateChannel(parser.Xdserver_addr_port,
                                grpc::InsecureChannelCredentials());
  channel_ctld = grpc::CreateChannel(parser.CtlXdserver_addr_port,
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
        signal(SIGINT, SendSignal);
        SrunXStreamReply reply;
        while (m_stream_->Read(&reply)) {
          if (reply.type() == SrunXStreamReply::IoRedirection) {
            SLURMX_INFO("reply_buf:{}", reply.io_redirection().buf());
          } else if (reply.type() == SrunXStreamReply::ExitStatus) {
            SLURMX_INFO("Srunxclient: Slurmxd exit for signal {}.",
                        reply.task_exit_status().reason());
            state = SrunX_State::FINISH;
          } else {
            if (reply.new_task_result().ok()) {
              SLURMX_INFO("New Task Success!");
            } else {
              SLURMX_INFO("New Task Failed, For the reason {} .",
                          reply.new_task_result().reason());
              err = SlurmxErr::kNewTaskFailed;
            }
          }
        }
      } break;

      case SrunX_State::ABORT:
        m_stream_->WritesDone();
        status = m_stream_->Finish();
        SLURMX_DEBUG("Connection from peer {} aborted.", context.peer());
        return err;

      case SrunX_State::FINISH:
        m_stream_->WritesDone();
        status = m_stream_->Finish();
        return err;
    }
  }
}

void SrunXClient::SendSignal(int signo) {
  SrunXStreamRequest request;
  request.set_type(SrunXStreamRequest::Signal);
  request.set_signum(2);
  m_stream_->Write(request);
}