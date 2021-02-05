#pragma once

#include <memory>
#include <thread>

#include "PublicHeader.h"
#include "grpc++/grpc++.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace SlurmXd {

using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using slurmx_grpc::SlurmXd;
using slurmx_grpc::SrunXStreamReply;
using slurmx_grpc::SrunXStreamRequest;

class SlurmXdServiceImpl final : public SlurmXd::Service {
  Status SrunXStream(ServerContext *context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>
                         *stream) override;
};

class XdServer {};

}  // namespace SlurmXd
