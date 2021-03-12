
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


int main(int argc, char** argv) {
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif
// Slurmctlxd server
  std::string server_ctld_address("0.0.0.0:50052");
  SrunCtldServiceImpl service_ctld;

  ServerBuilder builder_ctld;
  builder_ctld.AddListeningPort(server_ctld_address,
                                grpc::InsecureServerCredentials());
  builder_ctld.RegisterService(&service_ctld);
  std::unique_ptr<Server> server_ctld(builder_ctld.BuildAndStart());
  SLURMX_INFO("slurmctld Server listening on {}", server_ctld_address);


  server_ctld->Wait();

}














