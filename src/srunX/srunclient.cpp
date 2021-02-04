#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <csignal>
#include <cxxopts.hpp>
#include <grpcpp/grpcpp.h>
#include <fmt/format.h>
#include <thread>
#include <condition_variable>
#include <atomic>

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



//int main(int argc, char** argv) {
//
//  SrunXClient client(grpc::CreateChannel(
//      "localhost:50051", grpc::InsecureChannelCredentials()));
//  client.WriteValues(client.parse(argc,argv));
////  client.waitstop();
//
//
//  return 0;
//}
