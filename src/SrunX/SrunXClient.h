#include <grpcpp/grpcpp.h>
#include <string.h>

#include <atomic>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <condition_variable>
#include <csignal>
#include <cxxopts.hpp>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include "CommandLineParse.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"

namespace SrunX {

constexpr uint32_t kSrunVersion = 1;

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
  SrunXClient() = default;
  ~SrunXClient();

  SlurmxErr Init(std::string xd_addr_port, std::string ctlxd_addr_port);

  SlurmxErr Run(const CommandLineArgs &cmd_args);

  void Wait();

 private:
  static void SigintHandlerFunc(int);
  void SigintGrpcSendThreadFunc();

  std::unique_ptr<SlurmXd::Stub> m_xd_stub_;
  std::unique_ptr<SlurmCtlXd::Stub> m_ctld_stub_;

  inline static std::unique_ptr<
      grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
      m_stream_;
  ClientContext m_stream_context_;

  std::thread m_sigint_grpc_send_thread_;
  inline static std::atomic_bool s_sigint_received_{false};

  std::atomic_bool m_is_ending_{false};
  std::atomic_bool m_is_under_destruction_{false};

  std::shared_ptr<Channel> m_xd_channel_;
  std::shared_ptr<Channel> m_ctld_channel_;
};
}  // namespace SrunX