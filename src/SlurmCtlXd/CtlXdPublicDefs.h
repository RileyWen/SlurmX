#pragma once

#include <spdlog/fmt/bundled/format.h>

#include <boost/container_hash/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <string>
#include <unordered_map>

#include "PublicHeader.h"

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

namespace CtlXd {

/**
 * The static information on a Xd node (the static part of XDNodeData). This
 * structure is provided when a new Xd node is to be registered in
 * XdNodeMetaContainer.
 */
struct XdNodeStaticMeta {
  uint32_t node_index;  // Allocated when this node is up.
  std::string ipv4_addr;
  uint32_t port;

  std::string node_name;  // a node name corresponds to the node index

  uint32_t partition_id;  // Allocated if partition_name is new or
                          // use existing partition id of the partition_name.
  std::string partition_name;  // a partition_name corresponds to partition id.
  resource_t res;
};

/**
 * Represent the runtime status on a Xd node.
 * A Node is uniquely identified by (partition id, node index).
 */
struct XdNodeMeta {
  XdNodeStaticMeta static_meta;

  // total = avail + in-use
  resource_t res_total;  // A copy of res in XdNodeStaticMeta,
  // just for convenience.
  resource_t res_avail;
  resource_t res_in_use;

  // Store the information of the slices of allocated resource.
  // One uuid represents one shard of allocated resource.
  std::unordered_map<boost::uuids::uuid, resource_t,
                     boost::hash<boost::uuids::uuid>>
      resource_shards;
};

/**
 * A map from index to node information within a partition.
 */
using XdNodeMetaMap = std::unordered_map<uint32_t, XdNodeMeta>;

struct PartitionGlobalMeta {
  // total = avail + in-use
  resource_t m_resource_total_;
  resource_t m_resource_avail_;
  resource_t m_resource_in_use_;

  std::string name;

  // It is used to generate next node index for newly incoming Xd nodes in this
  // partition.
  uint32_t next_node_index = 0;
};

struct PartitionMetas {
  PartitionGlobalMeta partition_global_meta;
  XdNodeMetaMap xd_node_meta_map;
};

}  // namespace CtlXd