#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <grpcpp/grpcpp.h>

#include "protos/slurmx.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::ResourceAllocReply;
using slurmx_grpc::ResourceLimit;

// Logic and data behind the server's behavior.
class SrunCtldServiceImpl final : public SlurmCtlXd::Service {
  Status AllocateResource(ServerContext* context, const ResourceLimit* request,
                          ResourceAllocReply* reply) override {

    std::string uuid("e9ad48f9-1e60-497b-8d31-8a533a96f984");
    bool ok = true;
    if(ok){
      reply->set_ok(true);
      reply->set_resource_uuid(uuid);
    } else{
      reply->set_ok(false);
      reply->set_reason("reason why");
    }
    SLURMX_INFO("\nResourceLimit:\n cpu_byte: {}\n cpu_shares: {}\n memory_byte: {}\n memory_sw_byte: {}\n memory_ft_byte: {}\n blockio_wt_byte: {}\n",
                request->cpu_core_limit(),
                request->cpu_shares(),
                request->memory_limit_bytes(),
                request->memory_sw_limit_bytes(),
                request->memory_soft_limit_bytes(),
                request->blockio_weight()
    );
    return Status::OK;
  }
};

void RunServer() {

  std::string server_ctld_address("0.0.0.0:50052");
  SrunCtldServiceImpl service_ctld;

  ServerBuilder builder_ctld;
  builder_ctld.AddListeningPort(server_ctld_address, grpc::InsecureServerCredentials());
  builder_ctld.RegisterService(&service_ctld);
  std::unique_ptr<Server> server_ctld(builder_ctld.BuildAndStart());
  SLURMX_INFO("slurmctld Server listening on {}",server_ctld_address);

  server_ctld->Wait();
}

//int main(int argc, char** argv) {
//  RunServer();
//
//  return 0;
//}
