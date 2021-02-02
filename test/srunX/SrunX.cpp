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
#include "gtest/gtest.h"

#include "protos/slrumxd.grpc.pb.h"
#include "opt_parse.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SrunXRequest;
using slurmx_grpc::SrunXReply;



class SrunXClient : public opt_parse {
 public:
  explicit SrunXClient(const std::shared_ptr<Channel> &channel)
      : stub_(SlurmCtlXd::NewStub(channel)) {

    m_stream_ = stub_->SrunXStream(&m_context_);
    m_fg_=0;

    m_client_read_thread_=std::thread(&SrunXClient::m_client_read_func_, this);

    //wait signal
    m_client_wait_thread_=std::thread(&SrunXClient::m_client_wait_func_,this);
    signal(SIGINT, sig_int);


  }


  void WriteValues(const cxxopts::ParseResult& result){
    SrunXRequest request;

    //write Negotiation into stream
    uint32_t version =1;
    request.set_type(SrunXRequest::Negotiation);

    slurmx_grpc::Negotiation *negotiation=request.mutable_negotiation();
    slurmx_grpc::Negotiation nego;
    nego.set_version(version);
    negotiation->CopyFrom(nego);

    m_stream_->Write(request);

    //write new task into stream
    WriteNewTask(result);

    //stream status
    m_client_wait_thread_.join();
    m_client_read_thread_.join();


    m_stream_->WritesDone();
    Status status = m_stream_->Finish();

    if (!status.ok()) {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      std::cout << "RPC failed";
    }

  }


  void WriteNewTask(const cxxopts::ParseResult& result){
    SrunXRequest request;
    request.set_type(SrunXRequest::NewTask);

    slurmx_grpc::TaskInfo *taskInfo=request.mutable_task_info();
    slurmx_grpc::TaskInfo taskinfo;
    std::string str=result["task"].as<std::string>();
    taskinfo.set_executive_path(str);

    for(std::string arg : result["positional"].as<std::vector<std::string>>()){
      taskinfo.add_arguments(arg);
    }

    slurmx_grpc::TaskInfo_ResourceLimit *taskInfoResourceLimit=taskinfo.mutable_resource_limit();
    slurmx_grpc::TaskInfo_ResourceLimit taskresourcelimit;

    taskresourcelimit.set_cpu_core_limit(result["ncpu"].as<uint64_t>());
    taskresourcelimit.set_cpu_shares(result["ncpu_shares"].as<uint64_t>());
    taskresourcelimit.set_memory_limit_bytes(opt_parse::memory_parse_client("nmemory",result));
    taskresourcelimit.set_memory_sw_limit_bytes(opt_parse::memory_parse_client("nmemory_swap",result));
    taskresourcelimit.set_memory_soft_limit_bytes(opt_parse::memory_parse_client("nmemory_soft",result));
    taskresourcelimit.set_blockio_weight(opt_parse::memory_parse_client("blockio_weight",result));

    taskInfoResourceLimit->CopyFrom(taskresourcelimit);

    taskInfo->CopyFrom(taskinfo);

    m_stream_->Write(request);
  }

 private:

  void m_client_read_func_(){
    SrunXReply reply;
    while (m_stream_->Read(&reply)){
      std::cout << "S:"<< reply.io_redirection().buf() << "\n";
    }
  }
  void m_client_wait_func_(){
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_cv_.wait(lk,[]{return m_fg_==1;});
    WriteSignal();
    exit(0);
  }


  static void sig_int(int signo){
    printf("sig_int\n");
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_fg_=1;
    m_cv_.notify_all();
  }


  void WriteSignal(){

    SrunXRequest request;

    request.set_type(SrunXRequest::Signal);

    slurmx_grpc::Signal *Signal=request.mutable_signal();
    slurmx_grpc::Signal signal;
    signal.set_signal_type(slurmx_grpc::Signal::Interrupt);
    Signal->CopyFrom(signal);

    m_stream_->Write(request);
  }



  std::unique_ptr<SlurmCtlXd::Stub> stub_;
  static std::unique_ptr<grpc::ClientReaderWriter<SrunXRequest, SrunXReply>> m_stream_;
//  static volatile sig_atomic_t fg;
  static std::condition_variable m_cv_;
  static std::mutex m_cv_m_;
  static int m_fg_;
  std::thread m_client_read_thread_;
  std::thread m_client_wait_thread_;
  ClientContext m_context_;


};


std::condition_variable SrunXClient::m_cv_;
std::mutex SrunXClient::m_cv_m_;
//volatile sig_atomic_t SrunXClient::fg;
int SrunXClient::m_fg_;
std::unique_ptr<grpc::ClientReaderWriter<SrunXRequest, SrunXReply>> SrunXClient::m_stream_= nullptr;





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
          std::cout<<"Signal"<<std::endl;
          //TODO  print agrs
        }else if(request.type()==SrunXRequest::Negotiation){
          std::cout<<"Negotiation"<<std::endl;
          //TODO  print agrs
        } else if(request.type()==SrunXRequest::NewTask){
          std::cout<<"NewTask"<<std::endl;
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


//connection means tests
TEST(SrunX, StreamFromServer) {

  pid_t pid;
  int argc=1;
  char* argv[]={"./srun_test"};
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  SrunXClient client(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
  client.WriteValues(client.parse(argc,argv));


  server->Wait();



}


TEST(SrunX, SingalFromClient1) {

  pid_t pid;
  int argc=1;
  char* argv[]={"./srun_test"};
  std::string server_address("0.0.0.0:50051");
  SrunXServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  if((pid=fork()) == 0){
    SrunXClient client(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
    client.WriteValues(client.parse(argc,argv));

  }

  sleep(1);
  kill(pid,SIGINT);
  server->Wait();


}


//command line parse means tests
TEST(SrunX, OptHelpMessage)
{
  int argc=2;
  char* argv[]={"./srunX", "--help"};

  SrunXClient client(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  client.WriteValues(client.parse(argc,argv));

}


//command line parse means tests
TEST(SrunX, OptTestSimple)
{
  int argc=16;
  char* argv[]={"./srunX","-c", "10", "-s", "2", "-m" ,"200M", "-w", "102G", "-f", "100m", "-b", "1g", "task", "arg1", "arg2" };

  opt_parse parser;
  auto result = parser.parse(argc,argv);

  parser.PrintTaskInfo(parser.GetTaskInfo(result));

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
    std::cout<<"cpu input:"<<argv0[2]<<std::endl;
    auto result0 = parser.parse(argc,argv0);
    parser.GetTaskInfo(result0);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"cpu input:"<<argv1[2]<<std::endl;
    auto result1 = parser.parse(argc,argv1);
    parser.GetTaskInfo(result1);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"cpu input:"<<argv2[2]<<std::endl;
    auto result2 = parser.parse(argc,argv2);
    parser.GetTaskInfo(result2);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;
  try{
    std::cout<<"cpu input:"<<argv3[2]<<std::endl;
    auto result3 = parser.parse(argc,argv3);
    parser.GetTaskInfo(result3);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"cpu input:"<<argv4[2]<<std::endl;
    auto result4 = parser.parse(argc,argv4);
    parser.GetTaskInfo(result4);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"cpu input:"<<argv5[2]<<std::endl;
    auto result5 = parser.parse(argc,argv5);
    parser.GetTaskInfo(result5);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;
}


TEST(SrunX, OptTest_Memory)
{
  int argc=3;
  char* argv0[]={"./srunX","-m","m12"};
  char* argv1[]={"./srunX","-m","m12m"};
  char* argv2[]={"./srunX","-m","1m2"};
  char* argv3[]={"./srunX","-m","2.5m"};
  char* argv4[]={"./srunX","-m","125mm"};
  char* argv5[]={"./srunX","-m","125p"};

  opt_parse parser;


  try{
    std::cout<<"memory input:"<<argv0[2]<<std::endl;
    auto result0 = parser.parse(argc,argv0);
    parser.GetTaskInfo(result0);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"memory input:"<<argv1[2]<<std::endl;
    auto result1 = parser.parse(argc,argv1);
    parser.GetTaskInfo(result1);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"memory input:"<<argv2[2]<<std::endl;
    auto result2 = parser.parse(argc,argv2);
    parser.GetTaskInfo(result2);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;
  try{
    std::cout<<"memory input:"<<argv3[2]<<std::endl;
    auto result3 = parser.parse(argc,argv3);
    parser.GetTaskInfo(result3);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"memory input:"<<argv4[2]<<std::endl;
    auto result4 = parser.parse(argc,argv4);
    parser.GetTaskInfo(result4);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"memory input:"<<argv5[2]<<std::endl;
    auto result5 = parser.parse(argc,argv5);
    parser.GetTaskInfo(result5);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

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

  try{
    std::cout<<"task input:"<<argv0[1]<<std::endl;
    auto result0 = parser.parse(argc,argv0);
    parser.GetTaskInfo(result0);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"task input:"<<argv1[1]<<std::endl;
    auto result1 = parser.parse(argc,argv1);
    parser.GetTaskInfo(result1);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"task input:"<<argv2[1]<<std::endl;
    auto result2 = parser.parse(argc,argv2);
    parser.GetTaskInfo(result2);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"task input:"<<argv3[1]<<std::endl;
    auto result3 = parser.parse(argc,argv3);
    parser.GetTaskInfo(result3);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"task input:"<<argv4[1]<<std::endl;
    auto result4 = parser.parse(argc,argv4);
    parser.GetTaskInfo(result4);
  }catch (std::exception e){}

  std::cout<<"##############################"<<std::endl;

  try{
    std::cout<<"task input:"<<argv5[1]<<std::endl;
    auto result5 = parser.parse(argc,argv5);
    parser.GetTaskInfo(result5);
  }catch (std::exception e){}
}