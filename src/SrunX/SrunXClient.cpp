#include "SrunXClient.h"

SlurmxErr SrunXClient::Init(int argc, char** argv) {
  enum class SrunX_State {
    SEND_REQUIREMENT_TO_SLURMCTLXD = 0,
    NEGOTIATION_TO_SLURMXD,
    NEWTASK_TO_SLURMXD,
    WAIT_FOR_REPLY_OR_SEND_SIG,
    ABORT,
    FINISH
  };

  bool ok;
  err = SlurmxErr::kOk;
  SrunX_State state = SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD;
  ResourceAllocReply resourceAllocReply;
  ClientContext context;
  cxxopts::ParseResult result = parser.parse(argc, argv);
  Status status;
  m_stream_ = m_stub_->SrunXStream(&m_context_);
  m_fg_ = 0;

  while (true) {
    switch (state) {
      case SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD: {
        ResourceAllocRequest resourceAllocRequest;
        slurmx_grpc::AllocatableResource* AllocatableResource =
            resourceAllocRequest.mutable_required_resource();
        slurmx_grpc::AllocatableResource allocatableResource;
        allocatableResource.set_cpu_core_limit(result["ncpu"].as<uint64_t>());
        allocatableResource.set_memory_sw_limit_bytes(
            parser.memory_parse_client("nmemory_swap", result));
        allocatableResource.set_memory_limit_bytes(
            parser.memory_parse_client("nmemory", result));
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
            state = SrunX_State::NEGOTIATION_TO_SLURMXD;
          } else {
            SLURMX_DEBUG("Error ! Can not get token for reason: {}",
                         resourceAllocReply.reason());
            err = SlurmxErr::kNoTokenReply;
            state = SrunX_State::ABORT;
          }
        } else {
          SLURMX_DEBUG("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                       status.error_message());
          err = SlurmxErr::kConnectionFailed;
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
        std::string str = result["task"].as<std::string>();
        taskinfo.set_executive_path(str);

        for (std::string arg :
             result["positional"].as<std::vector<std::string>>()) {
          taskinfo.add_arguments(arg);
        }

        taskinfo.set_resource_uuid(resource_uuid.data, resource_uuid.size());
        taskInfo->CopyFrom(taskinfo);

        m_stream_->Write(request);
        state = SrunX_State::WAIT_FOR_REPLY_OR_SEND_SIG;
      } break;

      case SrunX_State::WAIT_FOR_REPLY_OR_SEND_SIG: {
        // wait the signal from user
        m_client_wait_thread_ =
            std::thread(&SrunXClient::m_client_wait_func_, this);
        signal(SIGINT, ModifySignalFlag);

        SrunXStreamReply reply;
        ok = m_stream_->Read(&reply);
        if (ok) {
          if (reply.type() == SrunXStreamReply::IoRedirection) {
            SLURMX_INFO("reply_buf:{}", reply.io_redirection().buf());
          } else if (reply.type() == SrunXStreamReply::ExitStatus) {
            if (reply.task_exit_status().reason() == TaskExitStatus::Signal) {
              SLURMX_INFO("Srunxclient: Slurmxd exit for sigint .");
            } else {
              SLURMX_INFO("Srunxclient: Slurmxd exit for normal .");
            }
            state = SrunX_State::FINISH;

          } else {
            if (reply.new_task_result().ok()) {
              SLURMX_DEBUG("New Task Success!");
            } else {
              SLURMX_DEBUG("New Task Failed, For the reason {} .",
                           reply.new_task_result().reason());
              err = SlurmxErr::kNewTaskFailed;
              state = SrunX_State::ABORT;
            }
          }
        } else {
          SLURMX_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context.peer());
          err = SlurmxErr::kConnectionFailed;
          state = SrunX_State::ABORT;
        }
        m_client_wait_thread_.join();
      } break;

      case SrunX_State::ABORT:
        SLURMX_INFO("abort");

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

void SrunXClient::m_client_wait_func_() {
  while (true) {
    if (m_fg_ == 1) {
      SrunXStreamRequest request;
      request.set_type(SrunXStreamRequest::Signal);
      request.set_signum(2);

      m_stream_->Write(request);
      break;
    }
  }
}

void SrunXClient::ModifySignalFlag(int signo) {
  SLURMX_DEBUG("Srunxclient: Press down 'Ctrl+C'");
  m_fg_ = 1;
}