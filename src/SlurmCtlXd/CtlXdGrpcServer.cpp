#include "CtlXdGrpcServer.h"

namespace CtlXd {

grpc::Status CtlXd::SlurmCtlXdServiceImpl::RegisterSlurmXd(
    grpc::ServerContext *context,
    const slurmx_grpc::SlurmXdRegisterRequest *request,
    slurmx_grpc::SlurmXdRegisterResult *response) {
  resource_t spec{request->resource_total()};

  SlurmxErr err;
  uuid node_uuid;

  std::vector<std::string> peer_slices;
  // ["ipv4", "<address>, "<port>"]
  std::string peer = context->peer();
  boost::algorithm::split(peer_slices, peer, boost::is_any_of(":"));
  m_ctlxd_server_->RegisterNewXd(
      fmt::format("{}:{}", peer_slices[1], request->port()), spec, &node_uuid);

  if (err == SlurmxErr::kOk) {
    response->set_ok(true);
    response->set_uuid(node_uuid.data, node_uuid.size());
  } else {
    response->set_ok(false);
    if (err == SlurmxErr::kConnectionTimeout)
      response->set_reason("CtlXd cannot connect to Xd backward");
    else
      response->set_reason(
          fmt::format("Unknown Failure: {}", SlurmxErrStr(err)));
  }

  return grpc::Status::OK;
}

grpc::Status SlurmCtlXdServiceImpl::AllocateResource(
    grpc::ServerContext *context,
    const slurmx_grpc::ResourceAllocRequest *request,
    slurmx_grpc::ResourceAllocReply *response) {
  resource_t res{request->required_resource()};

  SlurmxErr err;
  uuid res_uuid;
  err = m_ctlxd_server_->AllocateResource(res, &res_uuid);
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

  m_ctlxd_server_->HeartBeatFromNode(node_uuid);

  return grpc::Status::OK;
}

CtlXdServer::CtlXdServer(const std::string &listen_address)
    : m_listen_address_(listen_address) {
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

  SLURMX_TRACE(
      "Try registering a new node. Connecting backward to XdClient: {}",
      addr_port);

  SlurmxErr err;
  err = xd_client->Connect(addr_port);
  if (err != SlurmxErr::kOk) {
    // err MUST be kConnectionTimeout.
    SLURMX_TRACE("Failed to establish the channel to XdClient {}. Error: {}",
                 to_string(*xd_node_uuid), SlurmxErrStr(err));
    return err;
  }

  *xd_node_uuid = m_uuid_gen_();

  m_xd_stub_shared_mtx_.lock();
  m_xd_stub_maps_.emplace(*xd_node_uuid, std::move(xd_client));
  m_xd_stub_shared_mtx_.unlock();

  m_xd_node_mtx_.lock();

  m_resource_total_ += node_res;
  m_resource_avail_ += node_res;

  SlurmXdNode node{
      .node_uuid = *xd_node_uuid,
      .res_total = node_res,
      .res_avail = node_res,
      .res_in_use = {},
  };
  m_xd_node_map_.emplace(*xd_node_uuid, std::move(node));

  m_xd_node_mtx_.unlock();

  SLURMX_INFO("Node {} registered. cpu: {}, mem: {}, mem+sw: {}",
              to_string(*xd_node_uuid), node_res.cpu_count,
              node_res.memory_bytes, node_res.memory_sw_bytes);

  return SlurmxErr::kOk;
}

SlurmxErr CtlXdServer::AllocateResource(const resource_t &res, uuid *res_uuid) {
  std::lock_guard<std::mutex> guard(m_xd_node_mtx_);

  SLURMX_TRACE("Trying Allocating resource: cpu {}, mem: {}, mem+sw: {}",
               res.cpu_count, res.memory_bytes, res.memory_sw_bytes);
  if (m_resource_avail_ < res) {
    SLURMX_TRACE("Resource not enough. Avail: cpu {}, mem: {}, mem+sw: {}",
                 m_resource_avail_.cpu_count, m_resource_avail_.memory_bytes,
                 m_resource_avail_.memory_sw_bytes);
    return SlurmxErr::kNoResource;
  }

  for (auto &&[uuid, node] : m_xd_node_map_) {
    if (res <= node.res_avail) {
      // Todo: We should query the node to test if the required resource
      //  does not exceed the remaining resource on the node.
      //  Slurm seems to work in this way.

      *res_uuid = m_uuid_gen_();
      node.res_in_use += res;
      node.res_avail -= res;

      node.resc_shards.emplace(*res_uuid, res);

      m_resource_in_use_ += res;
      m_resource_avail_ -= res;

      SLURMX_TRACE(
          "Resource allocated successfully. uuid: {}. Informing XdClient...",
          to_string(*res_uuid));

      m_xd_stub_shared_mtx_.lock_shared();
      try {
        auto &stub = m_xd_stub_maps_.at(node.node_uuid);
        return stub->GrantResourceToken(*res_uuid, res);
      } catch (std::out_of_range &e) {
        SLURMX_ERROR(
            "Non-existent Xd uuid {} when looking up stub for "
            "GrantResourceToken",
            to_string(node.node_uuid));

        return SlurmxErr::kGenericFailure;
      }
    }
  }

  return SlurmxErr::kNoResource;
}

void CtlXdServer::HeartBeatFromNode(const uuid &node_uuid) {}

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

SlurmxErr XdClient::GrantResourceToken(const uuid &resource_uuid,
                                       const resource_t &resource) {
  using grpc::ClientContext;
  using grpc::Status;
  using slurmx_grpc::AllocatableResource;
  using slurmx_grpc::GrantResourceTokenReply;
  using slurmx_grpc::GrantResourceTokenRequest;

  GrantResourceTokenRequest req;

  req.set_resource_uuid(resource_uuid.data, resource_uuid.size());

  AllocatableResource *alloc_res = req.mutable_allocated_resource();
  alloc_res->set_cpu_core_limit(resource.cpu_count);
  alloc_res->set_memory_limit_bytes(resource.memory_bytes);
  alloc_res->set_memory_sw_limit_bytes(resource.memory_sw_bytes);

  ClientContext context;
  GrantResourceTokenReply resp;
  Status status;
  status = m_stub_->GrantResourceToken(&context, req, &resp);

  if (!status.ok()) {
    SLURMX_DEBUG("GrantResourceToken RPC returned with status not ok: {}",
                 status.error_message());
  }

  if (!resp.ok()) {
    SLURMX_DEBUG("GrantResourceToken got ok 'false' from XdClient: {}",
                 resp.reason());
    return SlurmxErr::kGenericFailure;
  }

  return SlurmxErr::kOk;
}
}  // namespace CtlXd