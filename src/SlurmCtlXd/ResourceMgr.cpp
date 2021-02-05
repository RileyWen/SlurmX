#include "ResourceMgr.h"

namespace CtlXd {

SlurmxErr ResourceMgr::RegisterNewSlurmXdNode(const resource_t &spec,
                                              uuid *node_uuid) {
  std::lock_guard<std::mutex> guard(m_mut_);

  m_resource_total_ += spec;
  m_resource_avail_ += spec;

  SlurmXdNode node{
      .node_uuid = *node_uuid,
      .res_total = spec,
      .res_avail = spec,
      .res_in_use = {},
  };

  *node_uuid = m_uuid_gen_();
  m_node_map_.emplace(*node_uuid, std::move(node));

  return SlurmxErr::OK;
}

SlurmxErr ResourceMgr::AllocateResource(const resource_t &res, uuid *res_uuid) {
  std::lock_guard<std::mutex> guard(m_mut_);

  if (m_resource_avail_ < res) return SlurmxErr::NO_RESOURCE;

  for (auto &&[uuid, node] : m_node_map_) {
    if (res <= node.res_avail) {
      *res_uuid = m_uuid_gen_();
      node.res_in_use += res;
      node.res_avail -= res;

      node.resc_shards.emplace(*res_uuid, res);

      return SlurmxErr::OK;
    }
  }

  return SlurmxErr::NO_RESOURCE;
}

void ResourceMgr::HeartBeatFromNode(const uuid &node_uuid) {}

}  // namespace CtlXd