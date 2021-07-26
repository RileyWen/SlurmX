#include "XdNodeMetaContainer.h"

#include "slurmx/String.h"

namespace CtlXd {

void XdNodeMetaContainerSimpleImpl::AddNode(
    const XdNodeStaticMeta& static_meta) {
  LockGuard guard(mtx_);

  uint32_t partition_id = GetPartitionId_(static_meta.partition_name);
  auto part_metas_iter = partition_metas_map_.find(partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // New partition. May be redundant here. See GetPartitionId_().
    PartitionMetas part_metas{
        .partition_global_meta = {.m_resource_total_ = static_meta.res,
                                  .m_resource_avail_ = static_meta.res,
                                  .name = static_meta.partition_name}};
    auto [part_metas_iter, ok] =
        partition_metas_map_.emplace(partition_id, std::move(part_metas));
    SLURMX_ASSERT(
        ok == true,
        "AddNode should never fails when creating a non-existent partition.");
  }

  PartitionMetas& part_metas = part_metas_iter->second;
  part_metas.partition_global_meta.m_resource_total_ += static_meta.res;
  part_metas.partition_global_meta.m_resource_avail_ += static_meta.res;

  SLURMX_DEBUG(
      "Add the resource of Node {} (cpu: {}, mem: {}) to partition [{}]'s "
      "global resource. partition [{}]'s Global resource now: "
      "cpu: {}, mem: {})",
      static_meta.node_index, static_meta.res.allocatable_resource.cpu_count,
      slurmx::ReadableMemory(static_meta.res.allocatable_resource.memory_bytes),
      static_meta.partition_name, static_meta.partition_name,
      part_metas.partition_global_meta.m_resource_total_.allocatable_resource
          .cpu_count,
      slurmx::ReadableMemory(part_metas.partition_global_meta.m_resource_total_
                                 .allocatable_resource.memory_bytes));

  XdNodeMeta node_meta{
      .static_meta = static_meta,
      .res_total = static_meta.res,
      .res_avail = static_meta.res,
      .res_in_use = {},
  };

  part_metas.xd_node_meta_map.emplace(static_meta.node_index,
                                      std::move(node_meta));
}

void XdNodeMetaContainerSimpleImpl::DeleteNodeMeta(XdNodeId node_id) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    SLURMX_ERROR(
        "Deleting non-existent node {} in DeleteNodeMeta: Partition not found",
        node_id);
    return;
  }

  XdNodeMetaMap& xd_node_meta_map = part_metas_iter->second.xd_node_meta_map;
  auto xd_node_meta_iter = xd_node_meta_map.find(node_id.node_index);
  if (xd_node_meta_iter == xd_node_meta_map.end()) {
    SLURMX_ERROR(
        "Deleting non-existent node {} in DeleteNodeMeta: Node not found in "
        "the partition",
        node_id);
    return;
  }

  PartitionGlobalMeta& part_meta =
      part_metas_iter->second.partition_global_meta;
  const XdNodeMeta& node_meta = xd_node_meta_iter->second;
  part_meta.m_resource_avail_ -= node_meta.res_avail;
  part_meta.m_resource_total_ -= node_meta.res_total;
  part_meta.m_resource_in_use_ -= node_meta.res_in_use;

  xd_node_meta_map.erase(node_id.node_index);
  if (xd_node_meta_map.empty()) {
    // The partition is empty now. Delete it.
    partition_name_id_map_.erase(part_meta.name);
    partition_metas_map_.erase(node_id.partition_id);
  }
}

XdNodeMetaContainerInterface::PartitionMetasPtr
XdNodeMetaContainerSimpleImpl::GetPartitionMetasPtr(uint32_t partition_id) {
  auto iter = partition_metas_map_.find(partition_id);
  if (iter == partition_metas_map_.end()) return {nullptr};

  mtx_.lock();

  return PartitionMetasPtr(&iter->second, &mtx_);
}

uint32_t XdNodeMetaContainerSimpleImpl::GetNextPartitionSeq_() {
  return partition_seq_++;
}

uint32_t XdNodeMetaContainerSimpleImpl::GetPartitionId(
    const std::string& partition_name) {
  LockGuard guard(mtx_);

  return GetPartitionId_(partition_name);
}

uint32_t XdNodeMetaContainerSimpleImpl::GetPartitionId_(
    const std::string& partition_name) {
  auto iter = partition_name_id_map_.find(partition_name);
  if (iter == partition_name_id_map_.end()) {
    uint32_t part_id = GetNextPartitionSeq_();
    partition_name_id_map_.emplace(partition_name, part_id);

    PartitionMetas part_metas{
        .partition_global_meta = {.name = partition_name}};
    auto [part_metas_iter, ok] =
        partition_metas_map_.emplace(part_id, std::move(part_metas));
    SLURMX_ASSERT(ok == true,
                  "GetPartitionId_ should never fails when creating a "
                  "non-existent partition.");
    return part_id;
  }

  return iter->second;
}

uint32_t XdNodeMetaContainerSimpleImpl::AllocNodeIndexInPartition(
    uint32_t partition_id) {
  LockGuard guard(mtx_);
  auto iter = partition_metas_map_.find(partition_id);
  if (iter == partition_metas_map_.end())
    return std::numeric_limits<uint32_t>::max();

  return iter->second.partition_global_meta.next_node_index++;
}

void XdNodeMetaContainerSimpleImpl::TryReleasePartition_(
    uint32_t partition_id) {
  auto part_metas_iter = partition_metas_map_.find(partition_id);
  if (part_metas_iter == partition_metas_map_.end())
    // No such partition.
    return;
  else {
    PartitionMetas& part_metas = part_metas_iter->second;
    if (part_metas.xd_node_meta_map.empty()) {
      partition_name_id_map_.erase(part_metas.partition_global_meta.name);
      partition_metas_map_.erase(partition_id);
    }
  }
}

void XdNodeMetaContainerSimpleImpl::TryReleasePartition(uint32_t partition_id) {
  LockGuard guard(mtx_);
  TryReleasePartition_(partition_id);
}

bool XdNodeMetaContainerSimpleImpl::PartitionExists(
    const std::string& partition_name) {
  LockGuard guard(mtx_);

  return partition_name_id_map_.count(partition_name) > 0;
}

XdNodeMetaContainerInterface::NodeMetaPtr
XdNodeMetaContainerSimpleImpl::GetNodeMetaPtr(XdNodeId node_id) {
  mtx_.lock();

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // No such partition.
    mtx_.unlock();
    return {nullptr};
  }

  auto node_meta_iter =
      part_metas_iter->second.xd_node_meta_map.find(node_id.node_index);
  if (node_meta_iter == part_metas_iter->second.xd_node_meta_map.end()) {
    // No such node in this partition.
    mtx_.unlock();
    return {nullptr};
  }

  return {&node_meta_iter->second, &mtx_};
}

XdNodeMetaContainerInterface::AllPartitionsMetaMapPtr
XdNodeMetaContainerSimpleImpl::GetAllPartitionsMetaMapPtr() {
  return {&partition_metas_map_, &mtx_};
}

}  // namespace CtlXd