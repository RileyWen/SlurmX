#pragma once

#include <grpc++/grpc++.h>

#include <boost/uuid/uuid.hpp>
#include <chrono>
#include <memory>

#include "PublicHeader.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace Xd {

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using SlurmxGrpc::SlurmCtlXd;
using SlurmxGrpc::SlurmXdRegisterRequest;
using SlurmxGrpc::SlurmXdRegisterResult;

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

  SlurmxErr RegisterOnCtlXd(const std::string& partition_name,
                            const AllocatableResource& resource,
                            uint32_t my_port);

  SlurmxErr DeallocateResource(const boost::uuids::uuid& resource_uuid);

  XdNodeId GetNodeId() const { return m_node_id_; };

 private:
  std::shared_ptr<Channel> m_ctlxd_channel_;

  std::unique_ptr<SlurmCtlXd::Stub> m_stub_;

  XdNodeId m_node_id_;
};

}  // namespace Xd

inline std::unique_ptr<Xd::CtlXdClient> g_ctlxd_client;