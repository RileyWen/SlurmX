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

#include "../src/SrunX/OptParse.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"

constexpr uint32_t kVersion = 1;

using boost::uuids::uuid;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::ResourceAllocReply;
using slurmx_grpc::ResourceAllocRequest;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;
using slurmx_grpc::TaskExitStatus;

class SrunXClient {
 public:
  //  SrunXClient(const std::shared_ptr<Channel>& channel,
  //              const std::shared_ptr<Channel>& channel_ctld)
  //      : m_stub_(SlurmXd::NewStub(channel)),
  //        m_stub_ctld_(SlurmCtlXd::NewStub(channel_ctld)) {}

  SrunXClient() = default;

  SlurmxErr Init(int argc, char* argv[]);

  SlurmxErr Run();

  enum class SrunX_State {
    SEND_REQUIREMENT_TO_SLURMCTLXD = 0,
    NEGOTIATION_TO_SLURMXD,
    NEWTASK_TO_SLURMXD,
    WAIT_FOR_REPLY_OR_SEND_SIG,
    ABORT,
    FINISH
  };

  OptParse parser;
  OptParse::AllocatableResource allocatableResource;
  OptParse::TaskInfo taskinfo;

 private:
  static void SendSignal(int signo);
  std::unique_ptr<SlurmXd::Stub> m_stub_;
  std::unique_ptr<SlurmCtlXd::Stub> m_stub_ctld_;
  static std::unique_ptr<
      grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
      m_stream_;
  SlurmxErr err;

  ClientContext m_context_;
  SrunX_State state;

  std::shared_ptr<Channel> channel;
  std::shared_ptr<Channel> channel_ctld;
};