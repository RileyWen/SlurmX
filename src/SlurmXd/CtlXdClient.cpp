//
// Created by rileywen on 2/5/21.
//

#include "CtlXdClient.h"

namespace SlurmXd {

SlurmxErr SlurmXd::CtlXdClient::RegisterOnCtlXd(const resource_t& resource) {
  SlurmXdNodeSpec spec;
  spec.set_cpu_count(resource.cpu_count);
  spec.set_memory_bytes(resource.memory_bytes);
  spec.set_memory_sw_bytes(resource.memory_sw_bytes);

  SlurmXdRegisterResult result;

  ClientContext context;
  Status status = m_stub_->RegisterSlurmXd(&context, spec, &result);

  if (status.ok()) {
    if (result.ok()) {
      std::copy(result.uuid().begin(), result.uuid().end(), m_node_uuid_.data);
      SLURMX_INFO("Register Node Successfully! UUID: {}",
                  boost::uuids::to_string(m_node_uuid_));

      return SlurmxErr::OK;
    }

    SLURMX_ERROR("Failed to register node. Reason from CtlXd: {}",
                 result.reason());
    return SlurmxErr::GENERIC_FAILURE;
  }

  SLURMX_ERROR("Register Error. Code: {}, Msg: {}", status.error_code(),
               status.error_message());
  return SlurmxErr::GENERIC_FAILURE;
}

}  // namespace SlurmXd
