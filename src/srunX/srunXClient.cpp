#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cxxopts.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../src/srunX/opt_parse.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"

#define version 1

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::ResourceAllocReply;
using slurmx_grpc::ResourceAllocRequest;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;

class SrunXClient {
 public:
  explicit SrunXClient(const std::shared_ptr<Channel> &channel,
                       const std::shared_ptr<Channel> &channel_ctld, int argc,
                       char *argv[])
      : m_stub_(SlurmXd::NewStub(channel)),
        m_stub_ctld_(SlurmCtlXd::NewStub(channel_ctld)) {
    enum class SrunX_State {
      SEND_REQUIREMENT_TO_SLURMCTLXD = 0,
      COMMUNICATION_WITH_SLURMXD,
      ABORT
    };

    SrunX_State state = SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD;
    ResourceAllocReply resourceAllocReply;
    ClientContext context;
    cxxopts::ParseResult result = parser.parse(argc, argv);

    while (true) {
      switch (state) {
        case SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD: {
          ResourceAllocRequest resourceAllocRequest;
          slurmx_grpc::AllocatableResource *AllocatableResource =
              resourceAllocRequest.mutable_required_resource();
          slurmx_grpc::AllocatableResource allocatableResource;
          allocatableResource.set_cpu_core_limit(result["ncpu"].as<uint64_t>());
          allocatableResource.set_memory_sw_limit_bytes(
              parser.memory_parse_client("nmemory_swap", result));
          allocatableResource.set_memory_limit_bytes(
              parser.memory_parse_client("nmemory", result));
          AllocatableResource->CopyFrom(allocatableResource);

          Status status = m_stub_ctld_->AllocateResource(
              &context, resourceAllocRequest, &resourceAllocReply);

          if (status.ok()) {
            if (resourceAllocReply.ok()) {
              m_uuid_ = resourceAllocReply.resource_uuid();
              SLURMX_INFO("Srunxclient: Get the token from the slurmxctld.");
              state = SrunX_State::COMMUNICATION_WITH_SLURMXD;
            } else {
              SLURMX_ERROR("Error ! Can not get token for reason: {}",
                           resourceAllocReply.reason());
              state = SrunX_State::ABORT;
            }
          } else {
            SLURMX_ERROR("{}:{}\nSlurmxctld RPC failed", status.error_code(),
                         status.error_message());
            state = SrunX_State::ABORT;
          }
        } break;

        case SrunX_State::COMMUNICATION_WITH_SLURMXD: {
          m_stream_ = m_stub_->SrunXStream(&m_context_);
          m_fg_ = 0;

          SrunXStreamRequest request;

          enum class SrunX_slurmd_State {
            NEGOTIATION = 0,
            NEW_TASK,
            WAIT_FOR_REPLY_OR_SEND_SIG,
            ABORT
          };

          SrunX_slurmd_State state_d = SrunX_slurmd_State::NEGOTIATION;
          while (true) {
            switch (state_d) {
              case SrunX_slurmd_State::NEGOTIATION: {
                request.set_type(SrunXStreamRequest::Negotiation);

                slurmx_grpc::Negotiation *negotiation =
                    request.mutable_negotiation();
                slurmx_grpc::Negotiation nego;
                nego.set_version(version);
                negotiation->CopyFrom(nego);

                m_stream_->Write(request);
                state_d = SrunX_slurmd_State::NEW_TASK;
              } break;

              case SrunX_slurmd_State::NEW_TASK: {
                SrunXStreamRequest request;
                request.set_type(SrunXStreamRequest::NewTask);

                slurmx_grpc::TaskInfo *taskInfo = request.mutable_task_info();
                slurmx_grpc::TaskInfo taskinfo;
                std::string str = result["task"].as<std::string>();
                taskinfo.set_executive_path(str);

                for (std::string arg :
                     result["positional"].as<std::vector<std::string>>()) {
                  taskinfo.add_arguments(arg);
                }

                taskinfo.set_resource_uuid(m_uuid_);
                taskInfo->CopyFrom(taskinfo);

                m_stream_->Write(request);
                state_d = SrunX_slurmd_State::WAIT_FOR_REPLY_OR_SEND_SIG;
              } break;

              case SrunX_slurmd_State::WAIT_FOR_REPLY_OR_SEND_SIG: {
                // read the stream from slurmxd
                m_client_read_thread_ =
                    std::thread(&SrunXClient::m_client_read_func_, this);
                // wait the signal from user
                m_client_wait_thread_ =
                    std::thread(&SrunXClient::m_client_wait_func_, this);
                signal(SIGINT, sig_int);

                m_client_wait_thread_.join();
                m_client_read_thread_.join();

                m_stream_->WritesDone();
                Status status = m_stream_->Finish();

                if (!status.ok()) {
                  SLURMX_ERROR("{}:{} \nSlurmxd RPC failed",
                               status.error_code(), status.error_message());
                  state_d = SrunX_slurmd_State::ABORT;
                }
              } break;

              case SrunX_slurmd_State::ABORT:
                throw std::exception();
                break;
            }
          }
        } break;
        case SrunX_State::ABORT:
          throw std::exception();
          break;
      }
    }
  }

  opt_parse parser;

 private:
  void m_client_read_func_() {
    SrunXStreamReply reply;
    while (m_stream_->Read(&reply)) {
      if (reply.type() == SrunXStreamReply::IoRedirection) {
        SLURMX_INFO("Received:{}", reply.io_redirection().buf());
      } else if (reply.type() == SrunXStreamReply::ExitStatus) {
        SLURMX_INFO("Srunxclient: Slurmxd exit at {} ,for the reason : {}",
                    reply.task_exit_status().return_value(),
                    reply.task_exit_status().reason());
        exit(0);
      } else {
        // TODO Print NEW Task Result
        SLURMX_INFO("New Task Result {} ", reply.new_task_result().reason());
        exit(0);
      }
    }
  }
  void m_client_wait_func_() {
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_cv_.wait(lk, [] { return m_fg_ == 1; });
    SrunXStreamRequest request;

    request.set_type(SrunXStreamRequest::Signal);

    slurmx_grpc::Signal *Signal = request.mutable_signal();
    slurmx_grpc::Signal signal;
    signal.set_signal_type(slurmx_grpc::Signal::Interrupt);
    Signal->CopyFrom(signal);

    m_stream_->Write(request);
  }

  static void sig_int(int signo) {
    SLURMX_INFO("Srunxclient: Press down 'Ctrl+C'");
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_fg_ = 1;
    m_cv_.notify_all();
  }

  std::unique_ptr<SlurmXd::Stub> m_stub_;
  std::unique_ptr<SlurmCtlXd::Stub> m_stub_ctld_;
  static std::unique_ptr<
      grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
      m_stream_;
  static std::condition_variable m_cv_;
  static std::mutex m_cv_m_;
  static int m_fg_;
  std::thread m_client_read_thread_;
  std::thread m_client_wait_thread_;
  ClientContext m_context_;
  std::string m_uuid_;
};