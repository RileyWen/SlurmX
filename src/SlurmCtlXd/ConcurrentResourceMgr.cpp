#include "ConcurrentResourceMgr.h"

namespace CtlXd {

void ConcurrentResourceMgr::RegisterNewSlurmXdNode(const uuid &node_uuid,
                                                   const resource_t &spec) {
  std::lock_guard<std::mutex> guard(m_mut_);

  m_resource_total_ += spec;
  m_resource_avail_ += spec;

  SlurmXdNode node{
      .node_uuid = node_uuid,
      .res_total = spec,
      .res_avail = spec,
      .res_in_use = {},
  };

  m_node_map_.emplace(node_uuid, std::move(node));
}

SlurmxErr ConcurrentResourceMgr::AllocateResource(const resource_t &res,
                                                  uuid *res_uuid) {
  std::lock_guard<std::mutex> guard(m_mut_);

  if (m_resource_avail_ < res) return SlurmxErr::kNoResource;

  for (auto &&[uuid, node] : m_node_map_) {
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

      return SlurmxErr::kOk;
    }
  }

  return SlurmxErr::kNoResource;
}

void ConcurrentResourceMgr::HeartBeatFromNode(const uuid &node_uuid) {}

}  // namespace CtlXd