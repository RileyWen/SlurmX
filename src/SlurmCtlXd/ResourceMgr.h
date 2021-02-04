#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_hash.hpp>
#include <list>
#include <map>
#include <mutex>
#include <unordered_map>

#include "PublicHeader.h"

namespace CtlXd {

using namespace boost::uuids;

struct SlurmXdNodeSpec {
  uint32_t cpu_count;
  uint64_t memory_bytes;
};

struct ResourceShard {
  uint64_t cpu_core_limit;
  uint64_t cpu_shares;
  uint64_t memory_limit_bytes;
  uint64_t memory_sw_limit_bytes;
  uint64_t memory_soft_limit_bytes;
  uint64_t blockio_weight;
};

struct SlurmXdNode {
  uuid node_uuid;
  SlurmXdNodeSpec spec;
  std::unordered_map<uuid, ResourceShard> resc_shards;
};

// A class for
class ResourceMgr {
 public:
  ResourceMgr& GetInstance() {
    static ResourceMgr ins;
    return ins;
  }

  uuid RegisterNewSlurmXdNode(const SlurmXdNodeSpec& spec);

  uuid AllocateResourceForNode(const uuid& node_uuid,
                               const ResourceShard& resc_shard);

  void HeartBeatFromNode(const uuid& node_uuid);

  void UnregisterSlurmXdNode(const uuid& node_uuid);

 private:
  ResourceMgr() = default;

  struct {
    uint32_t cpu_count;
    uint64_t memory_bytes;
  } m_resource_total;

  std::unordered_map<uuid, SlurmXdNode> m_node_map_;
  std::mutex m_mut_;
};

}  // namespace CtlXd