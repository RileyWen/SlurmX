#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_hash.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>

#include "PublicHeader.h"
#include "grpc++/grpc++.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"

namespace SlurmXd {

using boost::uuids::uuid;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using slurmx_grpc::SlurmCtlXd;
using slurmx_grpc::SlurmXdNodeSpec;
using slurmx_grpc::SlurmXdRegisterResult;

class CtlXdClient {
 public:
  explicit CtlXdClient(const std::string& server_address)
      : m_ctlxd_channel_(grpc::CreateChannel(
            server_address, grpc::InsecureChannelCredentials())),
        m_stub_(SlurmCtlXd::NewStub(m_ctlxd_channel_)) {}

  // Todo: Add exception handling over bad connections!

  SlurmxErr RegisterOnCtlXd(const resource_t& resource);

  const uuid& GetNodeUuid() const { return m_node_uuid_; };

 private:
  std::shared_ptr<Channel> m_ctlxd_channel_;

  std::unique_ptr<SlurmCtlXd::Stub> m_stub_;

  uuid m_node_uuid_;
};

}  // namespace SlurmXd
