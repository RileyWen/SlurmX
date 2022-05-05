#include "XdNodeMetaContainer.h"

#include "slurmx/String.h"

namespace CtlXd {

void XdNodeMetaContainerSimpleImpl::NodeUp(const XdNodeId& node_id) {
  LockGuard guard(mtx_);

  SLURMX_ASSERT(partition_metas_map_.count(node_id.partition_id) > 0);
  auto& part_meta = partition_metas_map_.at(node_id.partition_id);

  SLURMX_ASSERT(part_meta.xd_node_meta_map.count(node_id.node_index) > 0);
  auto& node_meta = part_meta.xd_node_meta_map.at(node_id.node_index);
  node_meta.alive = true;

  part_meta.partition_global_meta.m_resource_total_ += node_meta.res_total;
  part_meta.partition_global_meta.m_resource_avail_ += node_meta.res_total;
  part_meta.partition_global_meta.alive_node_cnt++;
}

void XdNodeMetaContainerSimpleImpl::NodeDown(XdNodeId node_id) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    SLURMX_ERROR(
        "Deleting non-existent node {} in NodeDown: Partition not found",
        node_id);
    return;
  }

  XdNodeMetaMap& xd_node_meta_map = part_metas_iter->second.xd_node_meta_map;
  auto xd_node_meta_iter = xd_node_meta_map.find(node_id.node_index);
  if (xd_node_meta_iter == xd_node_meta_map.end()) {
    SLURMX_ERROR(
        "Deleting non-existent node {} in NodeDown: Node not found in "
        "the partition",
        node_id);
    return;
  }

  PartitionGlobalMeta& part_meta =
      part_metas_iter->second.partition_global_meta;
  XdNodeMeta& node_meta = xd_node_meta_iter->second;
  node_meta.alive = false;

  part_meta.m_resource_avail_ -= node_meta.res_avail;
  part_meta.m_resource_total_ -= node_meta.res_total;
  part_meta.m_resource_in_use_ -= node_meta.res_in_use;
  part_meta.alive_node_cnt++;
}

XdNodeMetaContainerInterface::PartitionMetasPtr
XdNodeMetaContainerSimpleImpl::GetPartitionMetasPtr(uint32_t partition_id) {
  mtx_.lock();

  auto iter = partition_metas_map_.find(partition_id);
  if (iter == partition_metas_map_.end()) {
    mtx_.unlock();
    return PartitionMetasPtr{nullptr};
  }
  return PartitionMetasPtr(&iter->second, &mtx_);
}

bool XdNodeMetaContainerSimpleImpl::GetPartitionId(
    const std::string& partition_name, uint32_t* partition_id) {
  LockGuard guard(mtx_);
  auto iter = partition_name_id_map_.find(partition_name);
  if (iter == partition_name_id_map_.end()) {
    return false;
  }

  *partition_id = iter->second;
  return true;
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
    return NodeMetaPtr{nullptr};
  }

  auto node_meta_iter =
      part_metas_iter->second.xd_node_meta_map.find(node_id.node_index);
  if (node_meta_iter == part_metas_iter->second.xd_node_meta_map.end()) {
    // No such node in this partition.
    mtx_.unlock();
    return NodeMetaPtr{nullptr};
  }

  return NodeMetaPtr{&node_meta_iter->second, &mtx_};
}

XdNodeMetaContainerInterface::AllPartitionsMetaMapPtr
XdNodeMetaContainerSimpleImpl::GetAllPartitionsMetaMapPtr() {
  mtx_.lock();
  return AllPartitionsMetaMapPtr{&partition_metas_map_, &mtx_};
}

void XdNodeMetaContainerSimpleImpl::MallocResourceFromNode(
    XdNodeId node_id, uint32_t task_id, const Resources& resources) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // No such partition.
    return;
  }

  auto node_meta_iter =
      part_metas_iter->second.xd_node_meta_map.find(node_id.node_index);
  if (node_meta_iter == part_metas_iter->second.xd_node_meta_map.end()) {
    // No such node in this partition.
    return;
  }

  node_meta_iter->second.running_task_resource_map.emplace(task_id, resources);
  part_metas_iter->second.partition_global_meta.m_resource_avail_ -= resources;
  part_metas_iter->second.partition_global_meta.m_resource_in_use_ += resources;
  node_meta_iter->second.res_avail -= resources;
  node_meta_iter->second.res_in_use += resources;
}

void XdNodeMetaContainerSimpleImpl::FreeResourceFromNode(XdNodeId node_id,
                                                         uint32_t task_id) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // No such partition.
    return;
  }

  auto node_meta_iter =
      part_metas_iter->second.xd_node_meta_map.find(node_id.node_index);
  if (node_meta_iter == part_metas_iter->second.xd_node_meta_map.end()) {
    // No such node in this partition.
    return;
  }

  auto resource_iter =
      node_meta_iter->second.running_task_resource_map.find(task_id);
  if (resource_iter == node_meta_iter->second.running_task_resource_map.end()) {
    // Invalid task_id
    return;
  }

  const Resources& resources = resource_iter->second;
  part_metas_iter->second.partition_global_meta.m_resource_avail_ += resources;
  part_metas_iter->second.partition_global_meta.m_resource_in_use_ -= resources;
  node_meta_iter->second.res_avail += resources;
  node_meta_iter->second.res_in_use -= resources;

  node_meta_iter->second.running_task_resource_map.erase(resource_iter);
}

void XdNodeMetaContainerSimpleImpl::InitFromConfig(const Config& config) {
  LockGuard guard(mtx_);

  uint32_t part_seq = 0;

  for (auto&& [part_name, partition] : config.Partitions) {
    SLURMX_TRACE("Parsing partition {}", part_name);

    Resources part_res;

    partition_name_id_map_[part_name] = part_seq;
    auto& part_meta = partition_metas_map_[part_seq];
    uint32_t node_index = 0;

    for (auto&& node_name : partition.nodes) {
      SLURMX_TRACE("Parsing node {}", node_name);

      auto& node_meta = part_meta.xd_node_meta_map[node_index];

      auto& static_meta = node_meta.static_meta;
      static_meta.res.allocatable_resource.cpu_count =
          config.Nodes.at(node_name)->cpu;
      static_meta.res.allocatable_resource.memory_bytes =
          config.Nodes.at(node_name)->memory_bytes;
      static_meta.res.allocatable_resource.memory_sw_bytes =
          config.Nodes.at(node_name)->memory_bytes;
      static_meta.hostname = node_name;
      static_meta.port = std::strtoul(kXdDefaultPort, nullptr, 10);
      static_meta.partition_id = part_seq;
      static_meta.partition_name = part_name;

      node_meta.res_total = static_meta.res;
      node_meta.res_avail = static_meta.res;

      node_hostname_part_id_map_[node_name] = part_seq;
      part_id_host_index_map_[std::make_pair(part_seq, node_name)] = node_index;

      SLURMX_DEBUG(
          "Add the resource of Node #{} {} (cpu: {}, mem: {}) to partition "
          "[{}]'s global resource.",
          node_index, node_name, static_meta.res.allocatable_resource.cpu_count,
          util::ReadableMemory(
              static_meta.res.allocatable_resource.memory_bytes),
          static_meta.partition_name);

      node_index++;

      part_res += static_meta.res;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.m_resource_total_inc_dead_ = part_res;
    part_meta.partition_global_meta.node_cnt = node_index;
    part_meta.partition_global_meta.nodelist_str = partition.nodelist_str;

    SLURMX_DEBUG(
        "partition [{}]'s Global resource now: cpu: {}, mem: {}). It has {} "
        "nodes.",
        part_name,
        part_meta.partition_global_meta.m_resource_total_inc_dead_
            .allocatable_resource.cpu_count,
        util::ReadableMemory(
            part_meta.partition_global_meta.m_resource_total_inc_dead_
                .allocatable_resource.memory_bytes),
        node_index);
    part_seq++;
  }
}

bool XdNodeMetaContainerSimpleImpl::CheckNodeAllowed(
    const std::string& hostname) {
  LockGuard guard(mtx_);

  if (node_hostname_part_id_map_.count(hostname) > 0) return true;

  return false;
}

bool XdNodeMetaContainerSimpleImpl::GetNodeId(const std::string& hostname,
                                              XdNodeId* node_id) {
  LockGuard guard(mtx_);

  auto it1 = node_hostname_part_id_map_.find(hostname);
  if (it1 == node_hostname_part_id_map_.end()) return false;
  uint32_t part_id = it1->second;

  auto it2 = part_id_host_index_map_.find(std::make_pair(part_id, hostname));
  if (it2 == part_id_host_index_map_.end()) return false;
  uint32_t node_index = it2->second;

  node_id->partition_id = part_id;
  node_id->node_index = node_index;

  return true;
}

SlurmxGrpc::QueryNodeInfoReply*
XdNodeMetaContainerSimpleImpl::QueryAllNodeInfo() {
  LockGuard guard(mtx_);

  auto* reply = new SlurmxGrpc::QueryNodeInfoReply;
  auto* list = reply->mutable_node_info_list();

  for (auto&& [part_name, part_meta] : partition_metas_map_) {
    for (auto&& [node_name, node_meta] : part_meta.xd_node_meta_map) {
      auto* node_info = list->Add();
      auto& alloc_res_total = node_meta.res_total.allocatable_resource;
      auto& alloc_res_in_use = node_meta.res_in_use.allocatable_resource;
      auto& alloc_res_avail = node_meta.res_avail.allocatable_resource;

      node_info->set_hostname(node_meta.static_meta.hostname);
      node_info->set_cpus(alloc_res_total.cpu_count);
      node_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
      node_info->set_free_cpus(alloc_res_avail.cpu_count);
      node_info->set_real_mem(alloc_res_total.memory_bytes);
      node_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
      node_info->set_free_mem(alloc_res_avail.memory_bytes);
      node_info->set_partition_name(node_meta.static_meta.partition_name);
      node_info->set_running_task_num(
          node_meta.running_task_resource_map.size());
      if (node_meta.alive)
        node_info->set_state(SlurmxGrpc::NodeInfo_NodeState_IDLE);
      else
        node_info->set_state(SlurmxGrpc::NodeInfo_NodeState_DOWN);
    }
  }

  return reply;
}

SlurmxGrpc::QueryNodeInfoReply* XdNodeMetaContainerSimpleImpl::QueryNodeInfo(
    const std::string& node_name) {
  LockGuard guard(mtx_);

  auto* reply = new SlurmxGrpc::QueryNodeInfoReply;
  auto* list = reply->mutable_node_info_list();

  auto it1 = node_hostname_part_id_map_.find(node_name);
  if (it1 == node_hostname_part_id_map_.end()) {
    return reply;
  }

  uint32_t part_id = it1->second;
  auto key = std::make_pair(part_id, node_name);
  auto it2 = part_id_host_index_map_.find(key);
  if (it2 == part_id_host_index_map_.end()) {
    return reply;
  }
  uint32_t node_index = it2->second;

  auto& node_meta = partition_metas_map_[part_id].xd_node_meta_map[node_index];
  auto* node_info = list->Add();
  auto& alloc_res_total = node_meta.res_total.allocatable_resource;
  auto& alloc_res_in_use = node_meta.res_in_use.allocatable_resource;
  auto& alloc_res_avail = node_meta.res_avail.allocatable_resource;

  node_info->set_hostname(node_meta.static_meta.hostname);
  node_info->set_cpus(alloc_res_total.cpu_count);
  node_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
  node_info->set_free_cpus(alloc_res_avail.cpu_count);
  node_info->set_real_mem(alloc_res_total.memory_bytes);
  node_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  node_info->set_free_mem(alloc_res_avail.memory_bytes);
  node_info->set_partition_name(node_meta.static_meta.partition_name);
  node_info->set_running_task_num(node_meta.running_task_resource_map.size());
  if (node_meta.alive)
    node_info->set_state(SlurmxGrpc::NodeInfo_NodeState_IDLE);
  else
    node_info->set_state(SlurmxGrpc::NodeInfo_NodeState_DOWN);

  return reply;
}

SlurmxGrpc::QueryPartitionInfoReply*
XdNodeMetaContainerSimpleImpl::QueryAllPartitionInfo() {
  LockGuard guard(mtx_);

  auto* reply = new SlurmxGrpc::QueryPartitionInfoReply;
  auto* list = reply->mutable_partition_info();

  for (auto&& [part_name, part_meta] : partition_metas_map_) {
    auto* part_info = list->Add();
    auto& alloc_res_total =
        part_meta.partition_global_meta.m_resource_total_inc_dead_
            .allocatable_resource;
    auto& alloc_res_avail =
        part_meta.partition_global_meta.m_resource_total_.allocatable_resource;
    auto& alloc_res_in_use =
        part_meta.partition_global_meta.m_resource_in_use_.allocatable_resource;
    auto& alloc_res_free =
        part_meta.partition_global_meta.m_resource_avail_.allocatable_resource;
    part_info->set_name(part_meta.partition_global_meta.name);
    part_info->set_total_nodes(part_meta.partition_global_meta.node_cnt);
    part_info->set_alive_nodes(part_meta.partition_global_meta.alive_node_cnt);
    part_info->set_total_cpus(alloc_res_total.cpu_count);
    part_info->set_avail_cpus(alloc_res_avail.cpu_count);
    part_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
    part_info->set_free_cpus(alloc_res_free.cpu_count);
    part_info->set_total_mem(alloc_res_total.memory_bytes);
    part_info->set_avail_mem(alloc_res_avail.memory_bytes);
    part_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
    part_info->set_free_mem(alloc_res_free.memory_bytes);

    if (part_meta.partition_global_meta.alive_node_cnt > 0)
      part_info->set_state(SlurmxGrpc::PartitionInfo_PartitionState_UP);
    else
      part_info->set_state(SlurmxGrpc::PartitionInfo_PartitionState_DOWN);

    part_info->set_hostlist(part_meta.partition_global_meta.nodelist_str);
  }

  return reply;
}

SlurmxGrpc::QueryPartitionInfoReply*
XdNodeMetaContainerSimpleImpl::QueryPartitionInfo(
    const std::string& partition_name) {
  LockGuard guard(mtx_);

  auto* reply = new SlurmxGrpc::QueryPartitionInfoReply;
  auto* list = reply->mutable_partition_info();

  auto it = partition_name_id_map_.find(partition_name);
  if (it == partition_name_id_map_.end()) return reply;

  auto& part_meta = partition_metas_map_.at(it->second);

  auto* part_info = list->Add();
  auto& alloc_res_total = part_meta.partition_global_meta
                              .m_resource_total_inc_dead_.allocatable_resource;
  auto& alloc_res_avail =
      part_meta.partition_global_meta.m_resource_total_.allocatable_resource;
  auto& alloc_res_in_use =
      part_meta.partition_global_meta.m_resource_in_use_.allocatable_resource;
  auto& alloc_res_free =
      part_meta.partition_global_meta.m_resource_avail_.allocatable_resource;
  part_info->set_name(part_meta.partition_global_meta.name);
  part_info->set_total_nodes(part_meta.partition_global_meta.node_cnt);
  part_info->set_alive_nodes(part_meta.partition_global_meta.alive_node_cnt);
  part_info->set_total_cpus(alloc_res_total.cpu_count);
  part_info->set_avail_cpus(alloc_res_avail.cpu_count);
  part_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
  part_info->set_free_cpus(alloc_res_free.cpu_count);
  part_info->set_total_mem(alloc_res_total.memory_bytes);
  part_info->set_avail_mem(alloc_res_avail.memory_bytes);
  part_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  part_info->set_free_mem(alloc_res_free.memory_bytes);

  if (part_meta.partition_global_meta.alive_node_cnt > 0)
    part_info->set_state(SlurmxGrpc::PartitionInfo_PartitionState_UP);
  else
    part_info->set_state(SlurmxGrpc::PartitionInfo_PartitionState_DOWN);

  part_info->set_hostlist(part_meta.partition_global_meta.nodelist_str);

  return reply;
}

}  // namespace CtlXd