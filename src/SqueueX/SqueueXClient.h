#include <grpcpp/grpcpp.h>
#include <unistd.h>
#include <absl/time/time.h>
#include <absl/time/clock.h>

#include "CmdArgsParse.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"

namespace SqueueX {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using SlurmxGrpc::SlurmCtlXd;
using SlurmxGrpc::SqueueXReply;
using SlurmxGrpc::SqueueXRequest;

class SqueueXClient {
 public:
  SqueueXClient() = default;

  SlurmxErr Init(std::string ctlxd_addr_port);

  SlurmxErr GetInfo(bool clear_old, SqueueParameters &params);

 private:
  // Send request and presents the response back from the server.
  SlurmxErr LoadJobByJobId(JobInfoMsg *job_info_ptr, uint32_t job_id,
                           uint16_t show_flags);

  SlurmxErr LoadJobs(absl::Time update_time, JobInfoMsg *job_info_ptr,
                     uint16_t show_flags);

  SlurmxErr LoadJobsByUserId(JobInfoMsg *job_info_ptr, uint32_t user_id,
                             uint16_t show_flags);

  SlurmxErr PrintJobArray(const JobInfoMsg &jobs);

  std::unique_ptr<SlurmCtlXd::Stub> stub_;
  ClientContext context;
  std::shared_ptr<Channel> channel_;
};

}  // namespace SqueueX