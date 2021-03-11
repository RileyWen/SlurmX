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

struct SlurmXdNode {
  uuid node_uuid;

  // total = avail + in-use
  resource_t res_total;
  resource_t res_avail;
  resource_t res_in_use;

  std::unordered_map<uuid, resource_t> resc_shards;
};

// A thread-safe class for the resource management of nodes.
// All methods in the class is thread-safe.
class ConcurrentResourceMgr {
 public:
  static ConcurrentResourceMgr& GetInstance() {
    static ConcurrentResourceMgr ins;
    return ins;
  }

  void RegisterNewSlurmXdNode(const uuid& node_uuid, const resource_t& spec);

  SlurmxErr AllocateResource(const resource_t& res, uuid* res_uuid);

  void HeartBeatFromNode(const uuid& node_uuid);

  // Use heartbeat timeout to decide the availability of a node
  // void UnregisterSlurmXdNode(const uuid& node_uuid);

 private:
  ConcurrentResourceMgr() = default;

  // total = avail + in-use
  resource_t m_resource_total_;
  resource_t m_resource_avail_;
  resource_t m_resource_in_use_;

  std::unordered_map<uuid, SlurmXdNode> m_node_map_;
  std::mutex m_mut_;

  boost::uuids::random_generator_mt19937 m_uuid_gen_;
};

}  // namespace CtlXd