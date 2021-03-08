//
// Created by rileywen on 2/5/21.
//

#include "CtlXdClient.h"

namespace Xd {

SlurmxErr CtlXdClient::RegisterOnCtlXd(const resource_t& resource) {
  SlurmXdRegisterRequest req;

  AllocatableResource* resource_total = req.mutable_resource_total();
  resource_total->set_cpu_core_limit(resource.cpu_count);
  resource_total->set_memory_limit_bytes(resource.memory_bytes);
  resource_total->set_memory_sw_limit_bytes(resource.memory_sw_bytes);

  SlurmXdRegisterResult result;

  ClientContext context;
  Status status = m_stub_->RegisterSlurmXd(&context, req, &result);

  if (status.ok()) {
    if (result.ok()) {
      std::copy(result.uuid().begin(), result.uuid().end(), m_node_uuid_.data);
      SLURMX_INFO("Register Node Successfully! UUID: {}",
                  boost::uuids::to_string(m_node_uuid_));

      return SlurmxErr::kOk;
    }

    SLURMX_ERROR("Failed to register node. Reason from CtlXd: {}",
                 result.reason());
    return SlurmxErr::kGenericFailure;
  }

  SLURMX_ERROR("Register Error. Code: {}, Msg: {}", status.error_code(),
               status.error_message());
  return SlurmxErr::kGenericFailure;
}

}  // namespace Xd
