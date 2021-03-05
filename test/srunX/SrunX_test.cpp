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
using slurmx_grpc::ResourceAllocRequest;
using slurmx_grpc::ResourceAllocReply;


class SrunXClient {
 public:
  explicit SrunXClient(const std::shared_ptr<Channel> &channel,const std::shared_ptr<Channel> &channel_ctld,int argc,char* argv[])
      : m_stub_(SlurmXd::NewStub(channel)),m_stub_ctld_(SlurmCtlXd::NewStub(channel_ctld)) {

    enum class SrunX_State {
      SEND_REQUIREMENT_TO_SLURMCTLXD=0,
      COMMUNICATION_WITH_SLURMXD,
      ABORT
    };

    SrunX_State state = SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD;

    ResourceAllocReply resourceAllocReply;

    ClientContext context;

    cxxopts::ParseResult result=parser.parse(argc,argv);

    while(true){
      switch (state) {
          //send resourcelimit to slurmctlxd and received token
        case SrunX_State::SEND_REQUIREMENT_TO_SLURMCTLXD:
        {

          ResourceAllocRequest resourceAllocRequest;
          slurmx_grpc::AllocatableResource *AllocatableResource=resourceAllocRequest.mutable_required_resource();
          slurmx_grpc::AllocatableResource allocatableResource;
          allocatableResource.set_cpu_core_limit(result["ncpu"].as<uint64_t>());
          allocatableResource.set_memory_sw_limit_bytes(parser.memory_parse_client("nmemory_swap",result));
          allocatableResource.set_memory_limit_bytes(parser.memory_parse_client("nmemory",result));
          AllocatableResource->CopyFrom(allocatableResource);

          Status status = m_stub_ctld_->AllocateResource(&context, resourceAllocRequest, &resourceAllocReply);

          if (status.ok()) {
            if(resourceAllocReply.ok()){
              m_uuid_ = resourceAllocReply.resource_uuid();
              SLURMX_INFO("Srunxclient: Get the token from the slurmxctld.");
              state = SrunX_State::COMMUNICATION_WITH_SLURMXD;
            } else{
              SLURMX_ERROR("Error ! Can not get token for reason: {}", resourceAllocReply.reason());
              state = SrunX_State::ABORT;
            }
          } else {
            SLURMX_ERROR("{}:{}\nSlurmxctld RPC failed",status.error_code(),status.error_message());
            state = SrunX_State::ABORT;
          }
        }
          break;

        //communication with slurmxd
        //SEND
        //  negotiation - the version of the program
        //  newtask - newtask info with token
        //  signal - signal from front-end
        //RECEIVE
        //  IoRedirection -
        //  ExitStatus -
        //  NewTaskResult -
        case SrunX_State::COMMUNICATION_WITH_SLURMXD:
        {
          m_stream_ = m_stub_->SrunXStream(&m_context_);
          m_fg_=0;

          SrunXStreamRequest request;

          enum class SrunX_slurmd_State {
            NEGOTIATION = 0,
            NEW_TASK,
            WAIT_FOR_REPLY_OR_SEND_SIG,
            ABORT
          };

          SrunX_slurmd_State state_d = SrunX_slurmd_State::NEGOTIATION;

          while (true){
            switch (state_d) {
              case SrunX_slurmd_State::NEGOTIATION: {
                request.set_type(SrunXStreamRequest::Negotiation);

                slurmx_grpc::Negotiation* negotiation =
                    request.mutable_negotiation();
                slurmx_grpc::Negotiation nego;
                nego.set_version(version);
                negotiation->CopyFrom(nego);

                m_stream_->Write(request);
                state_d = SrunX_slurmd_State::NEW_TASK;
              } break;

              case SrunX_slurmd_State::NEW_TASK: {
                SrunXStreamRequest request;
                request.set_type(SrunXStreamRequest::NewTask);

                slurmx_grpc::TaskInfo* taskInfo = request.mutable_task_info();
                slurmx_grpc::TaskInfo taskinfo;
                std::string str = result["task"].as<std::string>();
                taskinfo.set_executive_path(str);

                for (std::string arg :
                     result["positional"].as<std::vector<std::string>>()) {
                  taskinfo.add_arguments(arg);
                }

                taskinfo.set_resource_uuid(m_uuid_);
                taskInfo->CopyFrom(taskinfo);

                m_stream_->Write(request);
                state_d = SrunX_slurmd_State::WAIT_FOR_REPLY_OR_SEND_SIG;
              } break;

              case SrunX_slurmd_State::WAIT_FOR_REPLY_OR_SEND_SIG:
              {
                // read the stream from slurmxd
                m_client_read_thread_ =
                    std::thread(&SrunXClient::m_client_read_func_, this);
                // wait the signal from user
                m_client_wait_thread_ =
                    std::thread(&SrunXClient::m_client_wait_func_, this);
                signal(SIGINT, sig_int);

                m_client_wait_thread_.join();
                m_client_read_thread_.join();

                m_stream_->WritesDone();
                Status status = m_stream_->Finish();

                if (!status.ok()) {
                  SLURMX_ERROR("{}:{} \nSlurmxd RPC failed", status.error_code(),
                               status.error_message());
                  state_d = SrunX_slurmd_State::ABORT;
                }
            }
                break;

              case SrunX_slurmd_State::ABORT:
                throw std::exception();
                break;
            }
          }
        }
          break;


        case SrunX_State::ABORT:
          throw std::exception();
          break;
      }
    }


  }

    opt_parse parser;

 private:

  void m_client_read_func_(){
    SrunXStreamReply reply;
    while (m_stream_->Read(&reply)){
      if(reply.type()==SrunXStreamReply::IoRedirection){
        SLURMX_INFO("Received:{}",reply.io_redirection().buf());
      }else if(reply.type()==SrunXStreamReply::ExitStatus){
        SLURMX_INFO("Srunxclient: Slurmxd exit at {} ,for the reason : {}",reply.task_exit_status().return_value(),reply.task_exit_status().reason());
        exit(0);
      } else {
        //TODO Print NEW Task Result
        SLURMX_INFO("New Task Result {} ",reply.new_task_result().reason());
        exit(0);
      }
    }
  }
  void m_client_wait_func_(){
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_cv_.wait(lk,[]{return m_fg_==1;});
    SrunXStreamRequest request;

    request.set_type(SrunXStreamRequest::Signal);

    slurmx_grpc::Signal *Signal=request.mutable_signal();
    slurmx_grpc::Signal signal;
    signal.set_signal_type(slurmx_grpc::Signal::Interrupt);
    Signal->CopyFrom(signal);

    m_stream_->Write(request);
  }


  static void sig_int(int signo){
    SLURMX_INFO("Srunxclient: Press down 'Ctrl+C'");
    std::unique_lock<std::mutex> lk(m_cv_m_);
    m_fg_=1;
    m_cv_.notify_all();
  }


  std::unique_ptr<SlurmXd::Stub> m_stub_;
  std::unique_ptr<SlurmCtlXd::Stub> m_stub_ctld_;
  static std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>> m_stream_;
//  static volatile sig_atomic_t fg;
  static std::condition_variable m_cv_;
  static std::mutex m_cv_m_;
  static int m_fg_;
  std::thread m_client_read_thread_;
  std::thread m_client_wait_thread_;
  ClientContext m_context_;
  std::string m_uuid_;

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
//    SrunXStreamRequest request;
//
//    SrunXStreamReply reply;
//
//    std::thread read([stream, &request]() {
//      while (stream->Read(&request)){
////        SLURMX_INFO("WHILE");
//        if(request.type()==SrunXStreamRequest::Signal){
//          SLURMX_INFO("Signal");
//          //TODO  print agrs
//          exit(0);
//        }else if(request.type()==SrunXStreamRequest::Negotiation){
//          SLURMX_INFO("Negotiation");
//          //TODO  print agrs
//        } else if(request.type()==SrunXStreamRequest::NewTask){
//          std::string args;
//          for(auto  arg : request.task_info().arguments()){
//            args.append(arg).append(", ");
//          }
//          SLURMX_INFO("\nNewTask:\n Task name: {}\n Task args: {}\n uuid: {}\n",
//                      request.task_info().executive_path(),
//                      args,
//                      request.task_info().resource_uuid());
//        }
//      }
//    });
//
//    reply.set_type(SrunXStreamReply::IoRedirection);
//    slurmx_grpc::IoRedirection * ioRedirection=reply.mutable_io_redirection();
//    slurmx_grpc::IoRedirection  ioRed;
////    ioRed.set_buf("OK");
////    ioRedirection->CopyFrom(ioRed);
//
//    stream->Write(reply);
//
//    for(int i=0;i<50;i++){
//      ioRed.set_buf(std::to_string(i));
//      ioRedirection->CopyFrom(ioRed);
//      stream->Write(reply);
//
//    }
//    read.join();
//    return Status::OK;

    SLURMX_DEBUG("SrunX connects from {}", context->peer());

    enum class StreamState {
      NEGOTIATION = 0,
      NEW_TASK,
      WAIT_FOR_EOF_OR_SIG,
      ABORT
    };

    bool ok;
    SrunXStreamRequest request;
    SrunXStreamReply reply;

    StreamState state = StreamState::NEGOTIATION;
    while (true) {
      switch (state) {
        case StreamState::NEGOTIATION:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() !=
                slurmx_grpc::SrunXStreamRequest_Type_Negotiation) {
              SLURMX_DEBUG("Expect negotiation from peer {}, but none.",
                           context->peer());
              state = StreamState::ABORT;
            } else {
              SLURMX_INFO("Slurmxdserver: RECEIVE NEGOTIATION");
              state = StreamState::NEW_TASK;
            }
          } else {
            SLURMX_DEBUG(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::ABORT;
          }
          break;

        case StreamState::NEW_TASK:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_NewTask) {
              SLURMX_DEBUG("Expect new_task from peer {}, but none.",
                           context->peer());
              state = StreamState::ABORT;
            } else {
              SLURMX_INFO("Slurmxdserver: RECEIVE NEWTASK");
              state = StreamState::WAIT_FOR_EOF_OR_SIG;
            }
          } else {
            SLURMX_DEBUG(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::ABORT;
          }
          break;

        case StreamState::WAIT_FOR_EOF_OR_SIG:
          ok = stream->Read(&request);
          if (ok) {
            if (request.type() != slurmx_grpc::SrunXStreamRequest_Type_Signal) {
              SLURMX_DEBUG("Expect signal from peer {}, but none.",
                           context->peer());
              state = StreamState::ABORT;
            } else {
              if (request.signal().signal_type() ==
                  slurmx_grpc::Signal_SignalType_Interrupt) {
                // Todo: Send signal to task manager here and wait for result.
                SLURMX_INFO("Slurmxdserver: RECEIVE SIGNAL");

                reply.set_type(SrunXStreamReply::ExitStatus);
                  slurmx_grpc::TaskExitStatus * taskExitStatus=reply.mutable_task_exit_status();
                  slurmx_grpc::TaskExitStatus  taskExitStatus1;
                  taskExitStatus1.set_return_value(0);
                  taskExitStatus1.set_reason("SIGKILL");
                  taskExitStatus->CopyFrom(taskExitStatus1);
                  stream->Write(reply);
                return Status::OK;
              }
              return Status::OK;
            }
          } else {
            SLURMX_DEBUG(
                "Connection error when trying reading negotiation from peer {}",
                context->peer());
            state = StreamState::ABORT;
          }
          break;

        case StreamState::ABORT:
          SLURMX_DEBUG("Connection from peer {} aborted.", context->peer());
          return Status::CANCELLED;

        default:
          SLURMX_ERROR("Unexpected XdServer State: {}", state);
          return Status::CANCELLED;
      }
    }
  }
};

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::ResourceAllocReply;
using slurmx_grpc::ResourceAllocRequest;

// Logic and data behind the server's behavior.
class SrunCtldServiceImpl final : public SlurmCtlXd::Service {
  Status AllocateResource(ServerContext* context, const ResourceAllocRequest* request,
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

        SLURMX_INFO("Slrumctlxdserver: \nrequired_resource:\n cpu_byte: {}\n memory_byte: {}\n memory_sw_byte: {}\n",
                request->required_resource().cpu_core_limit(),
                request->required_resource().memory_limit_bytes(),
                request->required_resource().memory_sw_limit_bytes()
    );
    return Status::OK;
  }
};


//connection means tests
TEST(SrunX, ConnectionSimple){

  //Slurmctlxd server
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

  //command line
  int argc=16;
  char* argv[]={"./srunX","-c", "10", "-s", "2", "-m" ,"200M", "-w", "102G", "-f", "100m", "-b", "1g", "task", "arg1", "arg2" };


  std::thread shutdown([]() {
    sleep(5);
    kill(getpid(),SIGINT);
  });
  //srunX client
  SrunXClient client(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()),grpc::CreateChannel(
      "localhost:50052", grpc::InsecureChannelCredentials()),
                     argc,argv);

  //Simulate the user pressing ctrl+C
  shutdown.join();

  server->Wait();

  server_ctld->Wait();

}

//command line parse means tests
TEST(SrunX, OptHelpMessage)
{
  int argc=2;
  char* argv[]={"./srunX", "--help"};

  SrunXClient client(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()),grpc::CreateChannel(
      "localhost:50052", grpc::InsecureChannelCredentials()),argc,argv);

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