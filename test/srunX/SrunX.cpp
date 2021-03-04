#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <csignal>
#include <cxxopts.hpp>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <condition_variable>
#include <atomic>
#include "gtest/gtest.h"
#include "PublicHeader.h"

#include "protos/slurmx.grpc.pb.h"
#include "../src/srunX/opt_parse.h"

#define version 1

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamRequest;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::ResourceLimit;
using slurmx_grpc::ResourceAllocReply;


class SrunXClient {
 public:
  explicit SrunXClient(const std::shared_ptr<Channel> &channel,const std::shared_ptr<Channel> &channel_ctld)
      : stub_(SlurmXd::NewStub(channel)),stub_ctld_(SlurmCtlXd::NewStub(channel_ctld)) {}


  std::string GetToken(const cxxopts::ParseResult& result){

    ResourceLimit resourceLimit;
    resourceLimit.set_cpu_core_limit(result["ncpu"].as<uint64_t>());
    resourceLimit.set_cpu_shares(result["ncpu_shares"].as<uint64_t>());
    resourceLimit.set_memory_limit_bytes(parser.memory_parse_client("nmemory",result));
    resourceLimit.set_memory_soft_limit_bytes(parser.memory_parse_client("nmemory_soft",result));
    resourceLimit.set_memory_sw_limit_bytes(parser.memory_parse_client("nmemory_swap",result));
    resourceLimit.set_blockio_weight(parser.memory_parse_client("blockio_weight",result));

    ResourceAllocReply resourceAllocReply;

    ClientContext context;

    Status status = stub_ctld_->AllocateResource(&context, resourceLimit, &resourceAllocReply);

    if (status.ok()) {
      if(resourceAllocReply.ok()){
        uuid = resourceAllocReply.resource_uuid();
        SLURMX_INFO("Get the token from the slurmxctld.");
      } else{
        SLURMX_ERROR("Error ! Can not get token for reason: {}", resourceAllocReply.reason());
        throw std::exception();
      }
    } else {
      SLURMX_ERROR("{}:{}\nSlurmxctld RPC failed",status.error_code(),status.error_message());
      throw std::exception();
    }
  }

  void WriteAndReadValues(const cxxopts::ParseResult& result){
    m_stream_ = stub_->SrunXStream(&m_context_);
    m_fg_=0;

    //read the stream from slurmxd
    m_client_read_thread_=std::thread(&SrunXClient::m_client_read_func_, this);

    //wait the signal from user
    m_client_wait_thread_=std::thread(&SrunXClient::m_client_wait_func_,this);
    signal(SIGINT, sig_int);

    SrunXStreamRequest request;

    //write Negotiation into stream
    request.set_type(SrunXStreamRequest::Negotiation);

    slurmx_grpc::Negotiation *negotiation=request.mutable_negotiation();
    slurmx_grpc::Negotiation nego;
    nego.set_version(version);
    negotiation->CopyFrom(nego);

    m_stream_->Write(request);

    //write new task into stream
    WriteNewTask(result);

    m_client_wait_thread_.join();
    m_client_read_thread_.join();


    m_stream_->WritesDone();
    Status status = m_stream_->Finish();

    if (!status.ok()) {
      SLURMX_ERROR("{}:{}",status.error_code(),status.error_message());
      SLURMX_ERROR("Slurmxd RPC failed");
      throw std::exception();
    }

  }


  void WriteNewTask(const cxxopts::ParseResult& result){
    SrunXStreamRequest request;
    request.set_type(SrunXStreamRequest::NewTask);

    slurmx_grpc::TaskInfo *taskInfo=request.mutable_task_info();
    slurmx_grpc::TaskInfo taskinfo;
    std::string str=result["task"].as<std::string>();
    taskinfo.set_executive_path(str);

    for(std::string arg : result["positional"].as<std::vector<std::string>>()){
      taskinfo.add_arguments(arg);
    }

    taskinfo.set_resource_uuid(uuid);
    taskInfo->CopyFrom(taskinfo);

    m_stream_->Write(request);
  }


    opt_parse parser;

 private:

  void m_client_read_func_(){
    SrunXStreamReply reply;
    while (m_stream_->Read(&reply)){
      if(reply.type()==SrunXStreamReply::IoRedirection){
        SLURMX_DEBUG("Received:{}",reply.io_redirection().buf());
      }else if(reply.type()==SrunXStreamReply::ExitStatus){
        SLURMX_INFO("Slurmctld exit at {} ,for the reason : {}",reply.task_exit_status().return_value(),reply.task_exit_status().reason());
        throw std::exception();
      } else {
        //TODO
        SLURMX_INFO("New Task Result {} ",reply.new_task_result().reason());
      }
    }
  }
  void m_client_wait_func_(){
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_cv_.wait(lk,[]{return m_fg_==1;});
    WriteSignal();
    exit(0);
  }


  static void sig_int(int signo){
    SLURMX_INFO("Press down 'Ctrl+C'");
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_fg_=1;
    m_cv_.notify_all();
  }


  void WriteSignal(){

    SrunXStreamRequest request;

    request.set_type(SrunXStreamRequest::Signal);

    slurmx_grpc::Signal *Signal=request.mutable_signal();
    slurmx_grpc::Signal signal;
    signal.set_signal_type(slurmx_grpc::Signal::Interrupt);
    Signal->CopyFrom(signal);

    m_stream_->Write(request);

  }



  std::unique_ptr<SlurmXd::Stub> stub_;
  std::unique_ptr<SlurmCtlXd::Stub> stub_ctld_;
  static std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>> m_stream_;
//  static volatile sig_atomic_t fg;
  static std::condition_variable m_cv_;
  static std::mutex m_cv_m_;
  static int m_fg_;
  std::thread m_client_read_thread_;
  std::thread m_client_wait_thread_;
  ClientContext m_context_;
  std::string uuid;

};


std::condition_variable SrunXClient::m_cv_;
std::mutex SrunXClient::m_cv_m_;
//volatile sig_atomic_t SrunXClient::fg;
int SrunXClient::m_fg_;
std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>> SrunXClient::m_stream_= nullptr;





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


//connection means tests
TEST(SrunX, ConnectionSimpleserver){

  std::string server_ctld_address("0.0.0.0:50052");
  SrunCtldServiceImpl service_ctld;

  ServerBuilder builder_ctld;
  builder_ctld.AddListeningPort(server_ctld_address, grpc::InsecureServerCredentials());
  builder_ctld.RegisterService(&service_ctld);
  std::unique_ptr<Server> server_ctld(builder_ctld.BuildAndStart());
  SLURMX_INFO("slurmctld Server listening on {}",server_ctld_address);

  //slurmXd server
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  SLURMX_INFO("slurmd Server listening on {}",server_address);

  server->Wait();

  server_ctld->Wait();

}


TEST(SrunX, ConnectionSimpleclient){

  int argc=16;
  char* argv[]={"./srunX","-c", "10", "-s", "2", "-m" ,"200M", "-w", "102G", "-f", "100m", "-b", "1g", "task", "arg1", "arg2" };
  SrunXClient client(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()),grpc::CreateChannel(
      "localhost:50052", grpc::InsecureChannelCredentials()));

  std::thread shutdown([]() {
    sleep(5);
    kill(getpid(),SIGINT);
  });


  auto result = client.parser.parse(argc,argv);

  //get token from slurmxctld
  client.GetToken(result);

  //read stream and send singal
  client.WriteAndReadValues(result);

  //Simulate the user pressing ctrl+C
  shutdown.join();

}



//command line parse means tests
TEST(SrunX, OptHelpMessage)
{
  int argc=2;
  char* argv[]={"./srunX", "--help"};

  SrunXClient client(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()),grpc::CreateChannel(
      "localhost:50052", grpc::InsecureChannelCredentials()));

  auto result = client.parser.parse(argc,argv);

}

TEST(SrunX, OptTest_C)
{
  int argc=3;
  char* argv0[]={"./srunX","-c","0"};
  char* argv1[]={"./srunX","-c","-1"};
  char* argv2[]={"./srunX","-c","0.5"};
  char* argv3[]={"./srunX","-c","2m"};
  char* argv4[]={"./srunX","-c","m"};
  char* argv5[]={"./srunX","-c","0M1"};

  opt_parse parser;

  try{
    SLURMX_INFO("cpu input:{}",argv0[2]);
    auto result0 = parser.parse(argc,argv0);
    parser.GetREsourceLimit(result0);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("cpu input:{}",argv1[2]);
    auto result1 = parser.parse(argc,argv1);
    parser.GetREsourceLimit(result1);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("cpu input:{}",argv2[2]);
    auto result2 = parser.parse(argc,argv2);
    parser.GetREsourceLimit(result2);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("cpu input:{}",argv3[2]);
    auto result3 = parser.parse(argc,argv3);
    parser.GetREsourceLimit(result3);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("cpu input:{}",argv4[2]);
    auto result4 = parser.parse(argc,argv4);
    parser.GetREsourceLimit(result4);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("cpu input:{}",argv5[2]);
    auto result5 = parser.parse(argc,argv5);
    parser.GetREsourceLimit(result5);
  }catch (std::exception e){}

}


TEST(SrunX, OptTest_Memory)
{
  int argc=3;
  char* argv0[]={"./srunX","-m","m12"};
  char* argv1[]={"./srunX","-m","18446744073709551615m"};
  char* argv2[]={"./srunX","-m","0"};
  char* argv3[]={"./srunX","-m","2.5m"};
  char* argv4[]={"./srunX","-m","125mm"};
  char* argv5[]={"./srunX","-m","125p"};

  opt_parse parser;


  try{
   SLURMX_INFO("memory input:{}",argv0[2]);
    auto result0 = parser.parse(argc,argv0);
    parser.GetREsourceLimit(result0);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("memory input:{}",argv1[2]);
    auto result1 = parser.parse(argc,argv1);
    parser.GetREsourceLimit(result1);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("memory input:{}",argv2[2]);
    auto result2 = parser.parse(argc,argv2);
    parser.GetREsourceLimit(result2);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("memory input:{}",argv3[2]);
    auto result3 = parser.parse(argc,argv3);
    parser.GetREsourceLimit(result3);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("memory input:{}",argv4[2]);
    auto result4 = parser.parse(argc,argv4);
    parser.GetREsourceLimit(result4);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("memory input:{}",argv5[2]);
    auto result5 = parser.parse(argc,argv5);
    parser.GetREsourceLimit(result5);
  }catch (std::exception e){}

}

TEST(SrunX, OptTest_Task)
{
  int argc=3;
  char* argv0[]={"./srunX", "task."};
  char* argv1[]={"./srunX", "task-"};
  char* argv2[]={"./srunX", "task/"};
  char* argv3[]={"./srunX", "task\\"};
  char* argv4[]={"./srunX", "task|"};
  char* argv5[]={"./srunX", "task*"};

  opt_parse parser;
  std::string uuid;
  try{
    SLURMX_INFO("task input:{}",argv0[1]);
    auto result0 = parser.parse(argc,argv0);
    parser.GetTaskInfo(result0,uuid);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("task input:{}",argv1[1]);
    auto result1 = parser.parse(argc,argv1);
    parser.GetTaskInfo(result1,uuid);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("task input:{}",argv2[1]);
    auto result2 = parser.parse(argc,argv2);
    parser.GetTaskInfo(result2,uuid);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("task input:{}",argv3[1]);
    auto result3 = parser.parse(argc,argv3);
    parser.GetTaskInfo(result3,uuid);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("task input:{}",argv4[1]);
    auto result4 = parser.parse(argc,argv4);
    parser.GetTaskInfo(result4,uuid);
  }catch (std::exception e){}

  try{
    SLURMX_INFO("task input:{}",argv5[1]);
    auto result5 = parser.parse(argc,argv5);
    parser.GetTaskInfo(result5,uuid);
  }catch (std::exception e){}
}