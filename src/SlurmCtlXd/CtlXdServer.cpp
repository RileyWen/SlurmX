#include "CtlXdServer.h"

namespace CtlXd {

grpc::Status CtlXd::SlurmCtlXdServiceImpl::RegisterSlurmXd(
    grpc::ServerContext *context,
    const slurmx_grpc::SlurmXdRegisterRequest *request,
    slurmx_grpc::SlurmXdRegisterResult *response) {
  ResourceMgr &res_mgr = ResourceMgr::GetInstance();

  resource_t spec;
  spec.cpu_count = request->spec().cpu_count();
  spec.memory_bytes = request->spec().memory_bytes();
  spec.memory_sw_bytes = request->spec().memory_sw_bytes();

  SlurmxErr err;
  uuid node_uuid;
  err = res_mgr.RegisterNewSlurmXdNode(spec, &node_uuid);
  if (err == SlurmxErr::OK) {
    response->set_ok(true);
    response->set_uuid(node_uuid.data, node_uuid.size());
  } else {
    response->set_ok(false);
  }
  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::AllocateResource(
    grpc::ServerContext *context, const slurmx_grpc::ResourceLimit *request,
    slurmx_grpc::ResourceAllocReply *response) {
  ResourceMgr &res_mgr = ResourceMgr::GetInstance();

  resource_t res;
  res.cpu_count = request->cpu_core_limit();
  res.memory_bytes = request->memory_limit_bytes();
  res.memory_sw_bytes = request->memory_sw_limit_bytes();

  SlurmxErr err;
  uuid res_uuid;
  err = res_mgr.AllocateResource(res, &res_uuid);
  if (err == SlurmxErr::OK) {
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

  ResourceMgr &res_mgr = ResourceMgr::GetInstance();
  res_mgr.HeartBeatFromNode(node_uuid);

  return grpc::Status::OK;
}
}  // namespace CtlXd