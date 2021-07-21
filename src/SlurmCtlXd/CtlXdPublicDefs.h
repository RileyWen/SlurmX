#pragma once

#include <boost/container_hash/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <string>
#include <unordered_map>

#include "PublicHeader.h"

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

namespace CtlXd {

struct GlobalMeta {
  // total = avail + in-use
  resource_t m_resource_total_;
  resource_t m_resource_avail_;
  resource_t m_resource_in_use_;
};

/**
 * Represent the runtime status on a Xd node.
 */
struct XdNodeMeta {
  uint32_t node_index;
  std::string ipv4_addr;
  uint32_t port;

  // total = avail + in-use
  resource_t res_total;
  resource_t res_avail;
  resource_t res_in_use;

  // Store the information of the slices of allocated resource.
  // One uuid represents one shard of allocated resource.
  std::unordered_map<boost::uuids::uuid, resource_t,
                     boost::hash<boost::uuids::uuid>>
      resource_shards;
};

/**
 * The static information on a Xd node (the static part of XDNodeData). This
 * structure is provided when a new Xd node is to be registered in
 * XdNodeMetaContainer.
 */
struct XdNodeStaticMeta {
  uint32_t node_index;
  std::string ipv4_addr;
  uint32_t port;
  resource_t res;
};

/**
 * A map from index to node information.
 */
using XdNodeMetaMap = std::unordered_map<uint32_t, XdNodeMeta>;

struct Metas {
  GlobalMeta global_meta;
  XdNodeMetaMap xd_node_meta_map;
};

}  // namespace CtlXd