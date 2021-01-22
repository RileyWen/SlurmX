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

#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"

namespace SrunX {
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
  SrunXClient() = default;

  SlurmxErr Init(std::string Xdserver_addr_port,
                 std::string CtlXdserver_addr_port);

  SlurmxErr Run();

  void Wait();

  enum class SrunX_State {
    SEND_REQUIREMENT_TO_SLURMCTLXD = 0,
    NEGOTIATION_TO_SLURMXD,
    NEWTASK_TO_SLURMXD,
    WAIT_FOR_REPLY_OR_SEND_SIG,
    ABORT,
    FINISH
  };

  struct AllocatableResource {
    uint64_t cpu_core_limit;
    uint64_t memory_limit_bytes;
    uint64_t memory_sw_limit_bytes;
  };

  struct TaskInfo {
    std::string executive_path;
    std::vector<std::string> arguments;
    uuid resource_uuid;
  };
  AllocatableResource allocatableResource;
  TaskInfo taskinfo;

 private:
  static void m_modify_signal_flag_(int signo);
  void m_client_wait_func_();

  std::unique_ptr<SlurmXd::Stub> m_stub_;
  std::unique_ptr<SlurmCtlXd::Stub> m_stub_ctld_;
  inline static std::unique_ptr<
      grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
      m_stream_;
  SlurmxErr m_err_;
  ClientContext m_context_;
  SrunX_State m_state_;
  std::thread m_client_wait_thread_;
  inline static std::condition_variable m_cv_;
  inline static std::mutex m_cv_m_;
  inline static std::atomic_int m_signal_fg_;
  inline static std::atomic_int m_exit_fg_;

  std::shared_ptr<Channel> m_channel_;
  std::shared_ptr<Channel> m_channel_ctld_;
};
}  // namespace SrunX