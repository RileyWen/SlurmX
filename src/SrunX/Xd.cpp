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

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;


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


int main(int argc, char** argv) {
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif
// slurmXd server
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl_Normal_EXit service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}", server_address);


  server->Wait();
}
