#include <iostream>
#include <cxxopts.hpp>

#include <fstream>
// #include"grpc_client_test.cpp"
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