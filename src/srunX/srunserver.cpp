#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <grpcpp/grpcpp.h>

#include "protos/slrumx.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SrunXRequest;
using slurmx_grpc::SrunXReply;


class SrunXServiceImpl final : public SlurmCtlXd::Service {
  Status SrunXStream(ServerContext* context,
                     ServerReaderWriter<SrunXReply, SrunXRequest>* stream) override {
    SrunXRequest request;

    SrunXReply reply;

    std::thread read([stream, &request]() {
      while (stream->Read(&request))
        if(request.type()==SrunXRequest::Signal){
          SLURMX_INFO("Signal");
          //TODO  print agrs
        }else if(request.type()==SrunXRequest::Negotiation){
          SLURMX_INFO("Negotiation");
          //TODO  print agrs
        } else if(request.type()==SrunXRequest::NewTask){
          SLURMX_INFO("NewTask");
          //TODO  print agrs
        }
    });

    reply.set_type(SrunXReply::IoRedirection);
    slurmx_grpc::IoRedirection * ioRedirection=reply.mutable_io_redirection();
    slurmx_grpc::IoRedirection  ioRed;
    ioRed.set_buf("OK");
    ioRedirection->CopyFrom(ioRed);

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
