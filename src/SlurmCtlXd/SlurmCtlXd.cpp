#include <memory>

#include "ConcurrentResourceMgr.h"
#include "CtlXdGrpcServer.h"
#include "PublicHeader.h"
#include "grpc++/grpc++.h"

int main() {
  std::string server_address{"localhost:50001"};
  CtlXd::SlurmCtlXdServiceImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server{builder.BuildAndStart()};
  SLURMX_INFO("Server listening on {}", server_address);

  server->Wait();

  return 0;
}