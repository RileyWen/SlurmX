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

  m_ctlxd_server_->RegisterNewXd(request->address_port(), spec, &node_uuid);

  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_uuid(node_uuid.data, node_uuid.size());
  } else {
    response->set_ok(false);
    if (err == SlurmxErr::kConnectionTimeout)
      response->set_reason("CtlXd cannot connect to Xd backward");
    else
      response->set_reason("Unknown Failure");
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

CtlXdServer::CtlXdServer(const std::string &listen_address)
    : m_listen_address_(listen_address),
      m_resource_mgr_(&ConcurrentResourceMgr::GetInstance()) {
  m_service_impl_ = std::make_unique<SlurmCtlXdServiceImpl>(this);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(m_listen_address_,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  SLURMX_INFO("SlurmCtlXd is listening on {}", m_listen_address_);

  // Avoid the potential deadlock error in underlying absl::mutex
  std::thread sigint_waiting_thread([p_server = m_server_.get()] {
    std::unique_lock<std::mutex> lk(s_sigint_mtx);
    s_sigint_cv.wait(lk);

    SLURMX_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");
    p_server->Shutdown();
  });
  sigint_waiting_thread.detach();

  signal(SIGINT, &CtlXdServer::signal_handler_func);
}

SlurmxErr CtlXdServer::RegisterNewXd(const std::string addr_port,
                                     const resource_t &node_res,
                                     uuid *xd_node_uuid) {
  auto xd_client = std::make_unique<XdClient>();

  SlurmxErr err;
  err = xd_client->Connect(addr_port);
  if (err != SlurmxErr::kOk) {
    // err MUST be kConnectionTimeout.
    return err;
  }

  *xd_node_uuid = m_uuid_gen_();

  m_xd_maps_mtx_.lock();
  m_xd_maps_.emplace(*xd_node_uuid, std::move(xd_client));
  m_xd_maps_mtx_.unlock();

  m_resource_mgr_->RegisterNewSlurmXdNode(*xd_node_uuid, node_res);

  return SlurmxErr::kOk;
}

SlurmxErr XdClient::Connect(const std::string &server_address) {
  m_xd_channel_ =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());

  using namespace std::chrono_literals;
  bool ok;
  ok = m_xd_channel_->WaitForConnected(std::chrono::system_clock::now() + 3s);
  if (!ok) {
    return SlurmxErr::kConnectionTimeout;
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = SlurmXd::NewStub(m_xd_channel_);

  return SlurmxErr::kOk;
}
}  // namespace CtlXd