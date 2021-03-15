#pragma once

#include <grpc++/grpc++.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_hash.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <memory>

#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace Xd {

using boost::uuids::uuid;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::AllocatableResource;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SlurmXdRegisterRequest;
using slurmx_grpc::SlurmXdRegisterResult;

class CtlXdClient {
 public:
  CtlXdClient() = default;

  /***
   * Connect the CtlXdClient to SlurmCtlXd.
   * @param server_address The "[Address]:[Port]" of SlurmCtlXd.
   * @return
   * If SlurmCtlXd is successfully connected, kOk is returned. <br>
   * If SlurmCtlXd cannot be connected within 3s, kConnectionTimeout is
   * returned.
   */
  SlurmxErr Connect(const std::string& server_address);

  SlurmxErr RegisterOnCtlXd(const resource_t& resource, uint32_t my_port);

  const uuid& GetNodeUuid() const { return m_node_uuid_; };

 private:
  std::shared_ptr<Channel> m_ctlxd_channel_;

  std::unique_ptr<SlurmCtlXd::Stub> m_stub_;

  uuid m_node_uuid_;
};

}  // namespace Xd

inline std::unique_ptr<Xd::CtlXdClient> g_ctlxd_client;