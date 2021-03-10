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

class SrunXServiceImpl final : public SlurmXd::Service {
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
              err = SlurmxErr::kOk;  // TODO

              if (err != SlurmxErr::kOk) {
                // The resource uuid provided by Client is invalid. Reject.

                reply.Clear();
                reply.set_type(
                    slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

                auto* result = reply.mutable_new_task_result();
                result->set_ok(false);
                result->set_reason("Resource uuid is invalid");

                stream->WriteLast(reply, grpc::WriteOptions());

                state = StreamState::kFinish;
              } else {
                err = SlurmxErr::kOk;  // TODO

                if (err == SlurmxErr::kOk) {
                  reply.Clear();
                  reply.set_type(
                      slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

                  auto* result = reply.mutable_new_task_result();
                  result->set_ok(true);

                  stream->Write(reply);
                  state = StreamState::kWaitForEofOrSigOrTaskEnd;
                } else {
                  reply.Clear();
                  reply.set_type(
                      slurmx_grpc::SrunXStreamReply_Type_NewTaskResult);

                  auto* result = reply.mutable_new_task_result();
                  result->set_ok(false);

                  if (err == SlurmxErr::kSystemErr)
                    result->set_reason(
                        fmt::format("System error: {}", strerror(errno)));
                  else if (err == SlurmxErr::kStop)
                    result->set_reason("Server is stopping");
                  else
                    result->set_reason(
                        fmt::format("Unknown failure. Code: ", uint16_t(err)));

                  stream->WriteLast(reply, grpc::WriteOptions());
                  state = StreamState::kFinish;
                }
              }
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

std::atomic_int SrunXClient::m_fg_;
SlurmxErr SrunXClient::err;
std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
    SrunXClient::m_stream_ = nullptr;

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
  EXPECT_EQ(client.Init(argc, const_cast<char**>(argv)), SlurmxErr::kOk);

  // Simulate the user pressing ctrl+C
  shutdown.join();

  server->Shutdown();
  server_ctld->Shutdown();

  server->Wait();
  server_ctld->Wait();
}
