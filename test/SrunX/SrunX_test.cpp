#include <grpcpp/grpcpp.h>

#include <atomic>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <condition_variable>
#include <csignal>
#include <cxxopts.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../src/SrunX/SrunXClient.h"
#include "gtest/gtest.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;

class SrunXServiceImpl_Ctrl_C_Interrupt final : public SlurmXd::Service {
  Status SrunXStream(ServerContext* context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>*
                         stream) override {
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

    std::string task_name;

    StreamState state = StreamState::kNegotiation;
    while (true) {
      switch (state) {
        case StreamState::kNegotiation:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
              SLURMX_INFO("Expect negotiation from peer {}, but none.",
                          context->peer());
              state = StreamState::kAbort;
            } else {
              state = StreamState::kNewTask;
            }
          } else {
            SLURMX_INFO(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::kAbort;
          }
          break;

        case StreamState::kNewTask:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
              SLURMX_INFO("Expect new_task from peer {}, but none.",
                          context->peer());
              state = StreamState::kAbort;
            } else {
              uuid resource_uuid;
              std::copy(request.task_info().resource_uuid().begin(),
                        request.task_info().resource_uuid().end(),
                        resource_uuid.data);

              SLURMX_DEBUG("{}", to_string(resource_uuid));
              reply.Clear();
              reply.set_type(slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

              auto* result = reply.mutable_new_task_result();
              result->set_ok(true);

              stream->Write(reply);

              state = StreamState::kWaitForEofOrSigOrTaskEnd;
            }
          } else {
            SLURMX_INFO(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::kAbort;
          }
          break;

        case StreamState::kWaitForEofOrSigOrTaskEnd: {
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_Signal) {
              SLURMX_INFO("Expect signal from peer {}, but none.",
                          context->peer());
              state = StreamState::kAbort;
            } else {
              slurmx_grpc::SrunXStreamReply reply;
              reply.set_type(slurmx_grpc::SrunXStreamReply_Type_ExitStatus);

              slurmx_grpc::TaskExitStatus* stat =
                  reply.mutable_task_exit_status();
              stat->set_reason(slurmx_grpc::TaskExitStatus_ExitReason_Signal);
              stat->set_value(1);

              stream->Write(reply);
              state = StreamState::kFinish;
            }
          } else {
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
};

class SrunXServiceImpl_Normal_EXit final : public SlurmXd::Service {
  Status SrunXStream(ServerContext* context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>*
                         stream) override {
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

    std::string task_name;

    StreamState state = StreamState::kNegotiation;
    while (true) {
      switch (state) {
        case StreamState::kNegotiation:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
              SLURMX_INFO("Expect negotiation from peer {}, but none.",
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
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
              SLURMX_INFO("Expect new_task from peer {}, but none.",
                          context->peer());
              state = StreamState::kAbort;
            } else {
              uuid resource_uuid;
              std::copy(request.task_info().resource_uuid().begin(),
                        request.task_info().resource_uuid().end(),
                        resource_uuid.data);

              SLURMX_DEBUG("{}", to_string(resource_uuid));
              reply.Clear();
              reply.set_type(slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

              auto* result = reply.mutable_new_task_result();
              result->set_ok(true);

              stream->Write(reply);
              state = StreamState::kWaitForEofOrSigOrTaskEnd;
            }
          } else {
            SLURMX_INFO(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::kAbort;
          }
          break;

        case StreamState::kWaitForEofOrSigOrTaskEnd: {
          slurmx_grpc::SrunXStreamReply reply;
          reply.set_type(slurmx_grpc::SrunXStreamReply_Type_ExitStatus);

          slurmx_grpc::TaskExitStatus* stat = reply.mutable_task_exit_status();
          stat->set_reason(slurmx_grpc::TaskExitStatus_ExitReason_Normal);
          stat->set_value(1);

          stream->Write(reply);
          state = StreamState::kFinish;
        } break;

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
};

class SrunXServiceImpl_NewTask_Failed final : public SlurmXd::Service {
  Status SrunXStream(ServerContext* context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>*
                         stream) override {
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

    std::string task_name;

    StreamState state = StreamState::kNegotiation;
    while (true) {
      switch (state) {
        case StreamState::kNegotiation:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
              SLURMX_INFO("Expect negotiation from peer {}, but none.",
                          context->peer());
              state = StreamState::kAbort;
            } else {
              state = StreamState::kNewTask;
            }
          } else {
            SLURMX_INFO(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::kAbort;
          }
          break;

        case StreamState::kNewTask:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
              SLURMX_INFO("Expect new_task from peer {}, but none.",
                          context->peer());
              state = StreamState::kAbort;
            } else {
              uuid resource_uuid;
              std::copy(request.task_info().resource_uuid().begin(),
                        request.task_info().resource_uuid().end(),
                        resource_uuid.data);

              SLURMX_DEBUG("{}", to_string(resource_uuid));
              reply.Clear();
              reply.set_type(slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

              auto* result = reply.mutable_new_task_result();
              result->set_ok(false);

              stream->Write(reply);
              state = StreamState::kWaitForEofOrSigOrTaskEnd;
            }
          } else {
            SLURMX_INFO(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::kAbort;
          }
          break;

        case StreamState::kWaitForEofOrSigOrTaskEnd: {
          slurmx_grpc::SrunXStreamReply reply;
          reply.set_type(slurmx_grpc::SrunXStreamReply_Type_ExitStatus);

          slurmx_grpc::TaskExitStatus* stat = reply.mutable_task_exit_status();
          stat->set_reason(slurmx_grpc::TaskExitStatus_ExitReason_Normal);
          stat->set_value(1);

          stream->Write(reply);
          state = StreamState::kFinish;
        } break;

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
    uuid resource_uuid = boost::uuids::random_generator()();
    bool ok = true;
    if (ok) {
      reply->set_ok(true);
      reply->set_resource_uuid(resource_uuid.data, resource_uuid.size());
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

class SrunCtldServiceImpl_No_Token_Reply final : public SlurmCtlXd::Service {
  Status AllocateResource(ServerContext* context,
                          const ResourceAllocRequest* request,
                          ResourceAllocReply* reply) override {
    uuid resource_uuid = boost::uuids::random_generator()();
    bool ok = false;
    if (ok) {
      reply->set_ok(true);
      reply->set_resource_uuid(resource_uuid.data, resource_uuid.size());
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

std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
    SrunXClient::m_stream_;

// connection means tests
TEST(SrunX, Ctrl_C_Interrupt) {
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
  SrunXServiceImpl_Ctrl_C_Interrupt service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}", server_address);

  // command line
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  //
  std::thread shutdown([]() {
    sleep(1);
    kill(getpid(), SIGINT);
  });
  SlurmxErr err;
  // srunX client
  SrunXClient client;
  err = client.Init(argc, const_cast<char**>(argv));
  EXPECT_EQ(client.Run(), SlurmxErr::kOk);

  // Simulate the user pressing ctrl+C
  shutdown.join();

  server->Shutdown();
  server_ctld->Shutdown();

  server->Wait();
  server_ctld->Wait();
}

TEST(SrunX, Normal_EXit) {
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
  SrunXServiceImpl_Normal_EXit service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}", server_address);
  // command line
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  // srunX client
  SrunXClient client;
  client.Init(argc, const_cast<char**>(argv));
  EXPECT_EQ(client.Run(), SlurmxErr::kOk);

  server->Shutdown();
  server_ctld->Shutdown();

  server->Wait();
  server_ctld->Wait();
}

TEST(SrunX, NewTask_Failed) {
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
  SrunXServiceImpl_NewTask_Failed service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}", server_address);

  // command line
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  // srunX client
  SrunXClient client;
  client.Init(argc, const_cast<char**>(argv));
  EXPECT_EQ(client.Run(), SlurmxErr::kNewTaskFailed);

  server->Shutdown();
  server_ctld->Shutdown();

  server->Wait();
  server_ctld->Wait();
}

TEST(SrunX, No_Token_Reply) {
  // Slurmctlxd server
  std::string server_ctld_address("0.0.0.0:50052");
  SrunCtldServiceImpl_No_Token_Reply service_ctld;

  ServerBuilder builder_ctld;
  builder_ctld.AddListeningPort(server_ctld_address,
                                grpc::InsecureServerCredentials());
  builder_ctld.RegisterService(&service_ctld);
  std::unique_ptr<Server> server_ctld(builder_ctld.BuildAndStart());
  SLURMX_INFO("slurmctld Server listening on {}", server_ctld_address);

  // slurmXd server
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl_Normal_EXit service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}", server_address);
  // command line
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  // srunX client
  SrunXClient client;
  client.Init(argc, const_cast<char**>(argv));
  EXPECT_EQ(client.Run(), SlurmxErr::kNoTokenReply);

  server->Shutdown();
  server_ctld->Shutdown();

  server->Wait();
  server_ctld->Wait();
}

TEST(SrunX, Connection_Failed) {
  // command line
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  // srunX client
  SrunXClient client;
  EXPECT_EQ(client.Init(argc, const_cast<char**>(argv)),
            SlurmxErr::kConnectionTimeout);
}