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
using grpc::ServerReaderWriter;
using grpc::Status;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamRequest;
using slurmx_grpc::SrunXStreamReply;


class SrunXServiceImpl final : public SlurmXd::Service {
  Status SrunXStream(ServerContext* context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>* stream) override {
    SrunXStreamRequest request;

    SrunXStreamReply reply;

    std::thread read([stream, &request]() {
      while (stream->Read(&request)){
//        SLURMX_INFO("WHILE");
        if(request.type()==SrunXStreamRequest::Signal){
          SLURMX_INFO("Signal");
          //TODO  print agrs
          exit(0);
        }else if(request.type()==SrunXStreamRequest::Negotiation){
          SLURMX_INFO("Negotiation");
          //TODO  print agrs
        } else if(request.type()==SrunXStreamRequest::NewTask){
          std::string args;
          for(auto  arg : request.task_info().arguments()){
            args.append(arg).append(", ");
          }
          SLURMX_INFO("\nNewTask:\n Task name: {}\n Task args: {}\n uuid: {}\n",
                      request.task_info().executive_path(),
                      args,
                      request.task_info().resource_uuid());
        }
      }
    });

    reply.set_type(SrunXStreamReply::IoRedirection);
    slurmx_grpc::IoRedirection * ioRedirection=reply.mutable_io_redirection();
    slurmx_grpc::IoRedirection  ioRed;
//    ioRed.set_buf("OK");
//    ioRedirection->CopyFrom(ioRed);

    stream->Write(reply);

    for(int i=0;i<50;i++){
      ioRed.set_buf(std::to_string(i));
      ioRedirection->CopyFrom(ioRed);
      stream->Write(reply);

    }
    read.join();
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case, it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

//int main(int argc, char** argv) {
//  RunServer();
//
//  return 0;
//}
