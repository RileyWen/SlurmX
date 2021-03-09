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
#include "gtest/gtest.h"
#include "protos/slurmx.grpc.pb.h"

constexpr uint32_t version = 1;

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
  explicit SrunXClient(const std::shared_ptr<Channel>& channel,
                       const std::shared_ptr<Channel>& channel_ctld)
      : m_stub_(SlurmXd::NewStub(channel)),
        m_stub_ctld_(SlurmCtlXd::NewStub(channel_ctld)) {}

  SlurmxErr Init(int argc, char* argv[]) {
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
          slurmx_grpc::AllocatableResource* AllocatableResource =
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
              SLURMX_DEBUG("Srunxclient: Get the token from the slurmxctld.");
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

                slurmx_grpc::Negotiation* negotiation =
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

                slurmx_grpc::TaskInfo* taskInfo = request.mutable_task_info();
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
                SlurmxErr err = SlurmxErr::OK;
                // read the stream from slurmxd
                m_client_read_thread_ = std::thread(
                    &SrunXClient::m_client_read_func_, this, std::ref(err));
                // wait the signal from user
                m_client_wait_thread_ =
                    std::thread(&SrunXClient::m_client_wait_func_, this);
                signal(SIGINT, sig_int);

                m_client_wait_thread_.join();
                m_client_read_thread_.join();

                m_stream_->WritesDone();
                Status status = m_stream_->Finish();
                return err;
              } break;
            }
          }
        } break;

        case SrunX_State::ABORT:
          return SlurmxErr::CONNECTION_FAILURE;
      }
    }
  }

  opt_parse parser;

 private:
  void m_client_read_func_(SlurmxErr& err) {
    SrunXStreamReply reply;
    while (m_stream_->Read(&reply)) {
      if (reply.type() == SrunXStreamReply::IoRedirection) {
        SLURMX_DEBUG("Received:{}", reply.io_redirection().buf());
      } else if (reply.type() == SrunXStreamReply::ExitStatus) {
        SLURMX_DEBUG("Srunxclient: Slurmxd exit at {} ,for the reason : {}",
                    reply.task_exit_status().return_value(),
                    reply.task_exit_status().reason());
        err = SlurmxErr::SIG_INT;
        break;
      } else {
        // TODO Print NEW Task Result
        SLURMX_DEBUG("New Task Result {} ", reply.new_task_result().reason());
      }
    }
  }
  void m_client_wait_func_() {
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_cv_.wait(lk, [] { return m_fg_ == 1; });
    SrunXStreamRequest request;

    request.set_type(SrunXStreamRequest::Signal);

    slurmx_grpc::Signal* Signal = request.mutable_signal();
    slurmx_grpc::Signal signal;
    signal.set_signal_type(slurmx_grpc::Signal::Interrupt);
    Signal->CopyFrom(signal);

    m_stream_->Write(request);
  }

  static void sig_int(int signo) {
    SLURMX_DEBUG("Srunxclient: Press down 'Ctrl+C'");
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
  static SlurmxErr err;
  std::thread m_client_read_thread_;
  std::thread m_client_wait_thread_;
  ClientContext m_context_;
  std::string m_uuid_;
};

std::condition_variable SrunXClient::m_cv_;
std::mutex SrunXClient::m_cv_m_;
int SrunXClient::m_fg_;
SlurmxErr SrunXClient::err;
std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
    SrunXClient::m_stream_ = nullptr;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;

class SrunXServiceImpl final : public SlurmXd::Service {
  Status SrunXStream(ServerContext* context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>*
                         stream) override {
    SLURMX_DEBUG("SrunX connects from {}", context->peer());

    enum class StreamState {
      NEGOTIATION = 0,
      NEW_TASK,
      WAIT_FOR_EOF_OR_SIG,
      ABORT
    };

    bool ok;
    SrunXStreamRequest request;
    SrunXStreamReply reply;

    StreamState state = StreamState::NEGOTIATION;
    while (true) {
      switch (state) {
        case StreamState::NEGOTIATION:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
              SLURMX_DEBUG("Expect negotiation from peer {}, but none.",
                           context->peer());
              state = StreamState::ABORT;
            } else {
              SLURMX_DEBUG("Slurmxdserver: RECEIVE NEGOTIATION");
              state = StreamState::NEW_TASK;
            }
          } else {
            SLURMX_DEBUG(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::ABORT;
          }
          break;

        case StreamState::NEW_TASK:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
              SLURMX_DEBUG("Expect new_task from peer {}, but none.",
                           context->peer());
              state = StreamState::ABORT;
            } else {
              SLURMX_DEBUG("Slurmxdserver: RECEIVE NEWTASK");
              state = StreamState::WAIT_FOR_EOF_OR_SIG;
            }
          } else {
            SLURMX_DEBUG(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::ABORT;
          }
          break;

        case StreamState::WAIT_FOR_EOF_OR_SIG:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_Signal) {
              SLURMX_DEBUG("Expect signal from peer {}, but none.",
                           context->peer());
              state = StreamState::ABORT;
            } else {
              if (request.signal().signal_type() ==
                  slurmx_grpc::Signal_SignalType_Interrupt) {
                // Todo: Send signal to task manager here and wait for result.
                SLURMX_DEBUG("Slurmxdserver: RECEIVE SIGNAL");

                reply.set_type(SrunXStreamReply::ExitStatus);
                slurmx_grpc::TaskExitStatus* taskExitStatus =
                    reply.mutable_task_exit_status();
                slurmx_grpc::TaskExitStatus taskExitStatus1;
                taskExitStatus1.set_return_value(0);
                taskExitStatus1.set_reason("SIGKILL");
                taskExitStatus->CopyFrom(taskExitStatus1);
                stream->Write(reply);
                return Status::OK;
              }
              return Status::OK;
            }
          } else {
            SLURMX_DEBUG(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::ABORT;
          }
          break;

        case StreamState::ABORT:
          SLURMX_DEBUG("Connection from peer {} aborted.", context->peer());
          return Status::CANCELLED;

        default:
          SLURMX_ERROR("Unexpected XdServer State: {}", state);
          return Status::CANCELLED;
      }
    }
  }
};

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using slurmx_grpc::ResourceAllocReply;
using slurmx_grpc::ResourceAllocRequest;
using slurmx_grpc::SlurmCtlXd;

class SrunCtldServiceImpl final : public SlurmCtlXd::Service {
  Status AllocateResource(ServerContext* context,
                          const ResourceAllocRequest* request,
                          ResourceAllocReply* reply) override {
    std::string uuid("e9ad48f9-1e60-497b-8d31-8a533a96f984");
    bool ok = true;
    if (ok) {
      reply->set_ok(true);
      reply->set_resource_uuid(uuid);
    } else {
      reply->set_ok(false);
      reply->set_reason("reason why");
    }
    SLURMX_DEBUG(
        "Slrumctlxdserver: \nrequired_resource:\n cpu_byte: {}\n memory_byte: "
        "{}\n memory_sw_byte: {}\n",
        request->required_resource().cpu_core_limit(),
        request->required_resource().memory_limit_bytes(),
        request->required_resource().memory_sw_limit_bytes());
    return Status::OK;
  }
};

// connection means tests
TEST(SrunX, ConnectionSimple) {
  // Slurmctlxd server
  std::string server_ctld_address("0.0.0.0:50052");
  SrunCtldServiceImpl service_ctld;

  ServerBuilder builder_ctld;
  builder_ctld.AddListeningPort(server_ctld_address,
                                grpc::InsecureServerCredentials());
  builder_ctld.RegisterService(&service_ctld);
  std::unique_ptr<Server> server_ctld(builder_ctld.BuildAndStart());
  SLURMX_INFO("slurmctld Server listening on {}", server_ctld_address);

  // slurmXd server
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}", server_address);

  // command line
  int argc = 16;
  const char* argv[] = {"./srunX", "-c",   "10",   "-s",  "2",    "-m",
                        "200M",    "-w",   "102G", "-f",  "100m", "-b",
                        "1g",      "task", "arg1", "arg2"};

  std::thread shutdown([]() {
    sleep(1);
    kill(getpid(), SIGINT);
  });
  // srunX client
  SrunXClient client(grpc::CreateChannel("localhost:50051",
                                         grpc::InsecureChannelCredentials()),
                     grpc::CreateChannel("localhost:50052",
                                         grpc::InsecureChannelCredentials()));
  EXPECT_EQ(client.Init(argc, const_cast<char**>(argv)), SlurmxErr::SIG_INT);

  // Simulate the user pressing ctrl+C
  shutdown.join();

  server->Shutdown();
  server_ctld->Shutdown();

  server->Wait();
  server_ctld->Wait();
}

// command line parse means tests
TEST(SrunX, OptHelpMessage) {
  int argc = 2;
  const char* argv[] = {"./srunX", "--help"};

  SrunXClient client(grpc::CreateChannel("localhost:50051",
                                         grpc::InsecureChannelCredentials()),
                     grpc::CreateChannel("localhost:50052",
                                         grpc::InsecureChannelCredentials()));
  client.Init(argc, const_cast<char**>(argv));
}
TEST(SrunX, OptTest_C_true) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-c", "10"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit,
            (uint64_t)atoi(argv[2]));
}

TEST(SrunX, OptTest_C_Zero) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-c", "0"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_C_negative) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "-1"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_C_decimal) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "0.5"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_C_errortype1) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "2m"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_C_errortype2) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "m"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_C_errortype3) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "0M1"};

  opt_parse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).cpu_core_limit ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_Memory_range) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "18446744073709551615m"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes ==
                (uint64_t)atoi(argv[2]),
            false);

  parser.GetAllocatableResource(result);
}

TEST(SrunX, OptTest_Memory_true_k) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes,
            (uint64_t)131072);
}

TEST(SrunX, OptTest_Memory_true_m) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128m"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes,
            (uint64_t)134217728);
}

TEST(SrunX, OptTest_Memory_true_g) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128g"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes,
            (uint64_t)137438953472);
}

TEST(SrunX, OptTest_Memory_zero) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "0"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_Memory_errortype1) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "m12"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_Memory_errortype2) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "2.5m"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes ==
                (uint64_t)atoi(argv[2]),
            false);
}
TEST(SrunX, OptTest_Memory_errortype4) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "125mm"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes ==
                (uint64_t)atoi(argv[2]),
            false);
}
TEST(SrunX, OptTest_Memory_errortype5) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "125p"};

  opt_parse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetAllocatableResource(result).memory_limit_bytes ==
                (uint64_t)atoi(argv[2]),
            false);
}

TEST(SrunX, OptTest_Task_true) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task"};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path, argv[1]);
}

TEST(SrunX, OptTest_Task_errortype1) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task."};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path == argv[1], false);
}

TEST(SrunX, OptTest_Task_errortype2) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task-"};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path == argv[1], false);
}

TEST(SrunX, OptTest_Task_errortype3) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task/"};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path == argv[1], false);
}

TEST(SrunX, OptTest_Task_errortype4) {
  int argc = 3;

  const char* argv[] = {"./srunX", "task\\"};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path == argv[1], false);
}

TEST(SrunX, OptTest_Task_errortype5) {
  int argc = 3;

  const char* argv[] = {"./srunX", "task|"};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path == argv[1], false);
}

TEST(SrunX, OptTest_Task_errortype6) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task*"};

  opt_parse parser;
  std::string uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  auto result = parser.parse(argc, const_cast<char**>(argv));
  EXPECT_EQ(parser.GetTaskInfo(result, uuid).executive_path == argv[1], false);
}