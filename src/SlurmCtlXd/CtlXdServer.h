#pragma once

#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace CtlXd {

class SlurmCtlXdServiceImpl final : public slurmx_grpc::SlurmCtlXd::Service {
  grpc::Status RegisterSlurmXd(
      grpc::ServerContext *context, const slurmx_grpc::SlurmXdNodeSpec *request,
      slurmx_grpc::SlurmXdRegisterResult *response) override {
    return grpc::Status::OK;
  }

  ::grpc::Status AllocateResource(
      ::grpc::ServerContext *context,
      const ::slurmx_grpc::ResourceLimit *request,
      ::slurmx_grpc::ResourceToken *response) override {}

  ::grpc::Status Heartbeat(::grpc::ServerContext *context,
                           const ::slurmx_grpc::HeartbeatRequest *request,
                           ::slurmx_grpc::HeartbeatReply *response) override {}
};

class CtlXdServer {};

}  // namespace CtlXd