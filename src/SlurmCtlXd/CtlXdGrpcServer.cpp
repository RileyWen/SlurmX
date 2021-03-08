#include "CtlXdGrpcServer.h"

namespace CtlXd {

grpc::Status CtlXd::SlurmCtlXdServiceImpl::RegisterSlurmXd(
    grpc::ServerContext *context,
    const slurmx_grpc::SlurmXdRegisterRequest *request,
    slurmx_grpc::SlurmXdRegisterResult *response) {
  ConcurrentResourceMgr &res_mgr = ConcurrentResourceMgr::GetInstance();

  resource_t spec{request->resource_total()};

  SlurmxErr err;
  uuid node_uuid;
  err = res_mgr.RegisterNewSlurmXdNode(spec, &node_uuid);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_uuid(node_uuid.data, node_uuid.size());
  } else {
    response->set_ok(false);
  }
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::AllocateResource(
    grpc::ServerContext *context,
    const slurmx_grpc::ResourceAllocRequest *request,
    slurmx_grpc::ResourceAllocReply *response) {
  ConcurrentResourceMgr &res_mgr = ConcurrentResourceMgr::GetInstance();

  resource_t res{request->required_resource()};

  SlurmxErr err;
  uuid res_uuid;
  err = res_mgr.AllocateResource(res, &res_uuid);
  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_resource_uuid(res_uuid.data, res_uuid.size());
  } else {
    response->set_ok(false);
    response->set_reason(SlurmxErrStr(err).data());
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::Heartbeat(
    grpc::ServerContext *context, const slurmx_grpc::HeartbeatRequest *request,
    slurmx_grpc::HeartbeatReply *response) {
  uuid node_uuid;
  std::copy(request->node_uuid().begin(), request->node_uuid().end(),
            node_uuid.data);

  ConcurrentResourceMgr &res_mgr = ConcurrentResourceMgr::GetInstance();
  res_mgr.HeartBeatFromNode(node_uuid);

  return grpc::Status::OK;
}
}  // namespace CtlXd