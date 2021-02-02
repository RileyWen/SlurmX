#ifndef _opt_parse_
#define _opt_parse_
#include <iostream>
#include <cxxopts.hpp>

#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include <fmt/format.h>

#include "protos/opt.pb.h"
#include "protos/opt.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurm_opt_grpc::Command;
using slurm_opt_grpc::Command_ResourceLimit;
using slurm_opt_grpc::CommandReply;
using slurm_opt_grpc::Submit;
class GreeterClient
{
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel);
  std::string submit(const cxxopts::ParseResult& result,std::uint64_t version=1);
  cxxopts::ParseResult parse(int argc, const char* argv[]);

 private:
  std::unique_ptr<Submit::Stub> stub_;
  uint64_t memory_parse_client(std::string str, const cxxopts::ParseResult &result);
};


#endif
