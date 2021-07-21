#include "XdNodeMetaContainer.h"

#include "slurmx/String.h"

namespace CtlXd {

void XdNodeMetaContainerSimpleImpl::AddNode(
    const XdNodeStaticMeta& static_meta) {
  LockGuard guard(mtx_);

  metas_.global_meta.m_resource_total_ += static_meta.res;
  metas_.global_meta.m_resource_avail_ += static_meta.res;

  SLURMX_DEBUG(
      "Add the resource of Node #{} (cpu: {}, mem: {}) to global resource. "
      "Global resource now: cpu: {}, mem: {})",
      static_meta.node_index, static_meta.res.cpu_count,
      slurmx::ReadableMemory(static_meta.res.memory_bytes),
      metas_.global_meta.m_resource_total_.cpu_count,
      slurmx::ReadableMemory(
          metas_.global_meta.m_resource_total_.memory_bytes));

  XdNodeMeta node_meta{
      .node_index = static_meta.node_index,
      .ipv4_addr = static_meta.ipv4_addr,
      .port = static_meta.port,
      .res_total = static_meta.res,
      .res_avail = static_meta.res,
      .res_in_use = {},
  };

  metas_.xd_node_meta_map.emplace(static_meta.node_index, std::move(node_meta));
}

void XdNodeMetaContainerSimpleImpl::DeleteNodeMeta(uint32_t index) {
  LockGuard guard(mtx_);

  auto iter = metas_.xd_node_meta_map.find(index);

  SLURMX_ASSERT(iter != metas_.xd_node_meta_map.end(),
                "Non-existent node index in DeleteNodeMeta!");

  const XdNodeMeta& node_meta = iter->second;
  metas_.global_meta.m_resource_avail_ -= node_meta.res_avail;
  metas_.global_meta.m_resource_total_ -= node_meta.res_total;
  metas_.global_meta.m_resource_in_use_ -= node_meta.res_in_use;

  metas_.xd_node_meta_map.erase(index);
}

XdNodeMetaContainerInterface::MetasPtr
XdNodeMetaContainerSimpleImpl::GetMetasPtr() {
  return MetasPtr(&metas_, &mtx_);
}

}  // namespace CtlXd