#pragma once

#include "ConcurrentResourceMgr.h"
#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace CtlXd {

class SlurmCtlXdServiceImpl final : public slurmx_grpc::SlurmCtlXd::Service {
  grpc::Status RegisterSlurmXd(
      grpc::ServerContext *context,
      const slurmx_grpc::SlurmXdRegisterRequest *request,
      slurmx_grpc::SlurmXdRegisterResult *response) override;

  grpc::Status AllocateResource(
      grpc::ServerContext *context,
      const slurmx_grpc::ResourceAllocRequest *request,
      slurmx_grpc::ResourceAllocReply *response) override;

  grpc::Status Heartbeat(grpc::ServerContext *context,
                         const slurmx_grpc::HeartbeatRequest *request,
                         slurmx_grpc::HeartbeatReply *response) override;
};

}  // namespace CtlXd