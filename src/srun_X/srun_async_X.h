//
// Created by slurm4 on 2021/1/27.
//

#ifndef SLURMX_SRUN_ASYNC_X_H
#define SLURMX_SRUN_ASYNC_X_H


#include <iostream>
#include <memory>
#include <string>
#include <cxxopts.hpp>
#include <fmt/format.h>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <thread>
#include <signal.h>

#include "protos/opt.pb.h"
#include "protos/opt.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using slurm_opt_grpc::Command;
using slurm_opt_grpc::Command_ResourceLimit;
using slurm_opt_grpc::CommandReply;
using slurm_opt_grpc::Submit;

class SubmiterClient {
 public:
  explicit SubmiterClient(std::shared_ptr<Channel> channel);
  // Assembles the client's payload and sends it to the server.
  void submit(const cxxopts::ParseResult& result,const std::uint32_t & version);
  // Loop while listening for completed responses.
  // Prints out the response from the server.
  void AsyncCompleteRpc();
  cxxopts::ParseResult parse(int argc, const char* argv[]);
 private:
  uint64_t memory_parse_client(std::string str, const cxxopts::ParseResult &result);
  // struct for keeping state and data information
  struct AsyncClientCall {
    // Container for the data we expect from the server.
    CommandReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // Storage for the status of the RPC upon completion.
    Status status;


    std::unique_ptr<ClientAsyncResponseReader<CommandReply>> response_reader;
  };

  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Submit::Stub> stub_;

  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq_;
};



#endif  // SLURMX_SRUN_ASYNC_X_H
