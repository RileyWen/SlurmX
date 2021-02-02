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

#include "protos/slrumxd.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SrunXRequest;
using slurmx_grpc::SrunXReply;



class SrunXClient {
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

  cxxopts::ParseResult  parse(int argc, char** argv){
    try
    {
      cxxopts::Options options(argv[0], " - srun command line options");
      options
          .positional_help("task_name [Task Args...]")
          .show_positional_help();
      options
          .add_options()
              ("c,ncpu", "limiting the cpu usage of task", cxxopts::value<uint64_t>()->default_value("2"))
              ("s,ncpu_shares", "limiting the cpu shares of task", cxxopts::value<uint64_t>()->default_value("2"))
              ("m,nmemory", "limiting the memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
              ("w,nmemory_swap", "limiting the swap memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
              ("f,nmemory_soft", "limiting the soft memory usage of task",cxxopts::value<std::string>()->default_value("128M"))
              ("b,blockio_weight", "limiting the weight of blockio",cxxopts::value<std::string>()->default_value("128M"))
              ("t,task", "task", cxxopts::value<std::string>()->default_value("notask"))
              ("help", "Print help")
              ("positional",
               "Positional arguments: these are the arguments that are entered "
               "without an option", cxxopts::value<std::vector<std::string>>()->default_value(" "));


      options.parse_positional({"task", "positional"});

      auto result = options.parse(argc, argv);

      if (result.count("help"))
      {
        fmt::print("{}",options.help({"", "Group"}));
        exit(0);
      }
      return result;
    }
    catch (const cxxopts::OptionException& e)
    {
      fmt::print("error parsing options: {}\n", e.what());
      exit(1);
    }
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
    taskresourcelimit.set_memory_limit_bytes(memory_parse_client("nmemory",result));
    taskresourcelimit.set_memory_sw_limit_bytes(memory_parse_client("nmemory_swap",result));
    taskresourcelimit.set_memory_soft_limit_bytes(memory_parse_client("nmemory_soft",result));
    taskresourcelimit.set_blockio_weight(memory_parse_client("blockio_weight",result));

    taskInfoResourceLimit->CopyFrom(taskresourcelimit);

    taskInfo->CopyFrom(taskinfo);

    m_stream_->Write(request);
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



 private:

  std::unique_ptr<SlurmCtlXd::Stub> stub_;
  uint64_t memory_parse_client(std::string str, const cxxopts::ParseResult &result){
    auto nmemory = result[str].as<std::string>();
    uint64_t nmemory_byte;
    if( nmemory[nmemory.length()-1]=='M' ||  nmemory[nmemory.length()-1] == 'm'){
      nmemory_byte =(uint64_t)std::stoi(nmemory.substr(0,nmemory.length()-1)) * 1024;
    }
    else if(nmemory[nmemory.length()-1]=='G' ||  nmemory[nmemory.length()-1]=='g'){
      nmemory_byte = (uint64_t)std::stoi(nmemory.substr(0,nmemory.length()-1)) * 1024 * 1024;
    }
    else{
      nmemory_byte=(uint64_t)std::stoi(nmemory.substr(0, nmemory.length()));
    }

    return nmemory_byte;
  }


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
