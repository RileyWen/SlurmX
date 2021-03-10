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

#include "../src/SrunX/opt_parse.h"
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
  SrunXClient(const std::shared_ptr<Channel>& channel,
              const std::shared_ptr<Channel>& channel_ctld)
      : m_stub_(SlurmXd::NewStub(channel)),
        m_stub_ctld_(SlurmCtlXd::NewStub(channel_ctld)) {}

  SlurmxErr Init(int argc, char* argv[]);

  opt_parse parser;

 private:
  void m_client_wait_func_();

  static void ModifySignalFlag(int signo);

  std::unique_ptr<SlurmXd::Stub> m_stub_;
  std::unique_ptr<SlurmCtlXd::Stub> m_stub_ctld_;
  static std::unique_ptr<
      grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
      m_stream_;
  static std::atomic_int m_fg_;
  static SlurmxErr err;
  std::thread m_client_wait_thread_;
  ClientContext m_context_;
  uuid resource_uuid{};
};