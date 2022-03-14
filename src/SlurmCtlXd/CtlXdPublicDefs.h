#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)

#include <boost/container_hash/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <chrono>
#include <string>
#include <unordered_map>
#include <variant>

#include "slurmx/PublicHeader.h"

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

namespace CtlXd {

constexpr uint64_t kTaskScheduleIntervalMs = 1000;       // Todo: Add comment
constexpr uint64_t kEndedTaskCleanIntervalSeconds = 1;   // Todo: Add comment
constexpr uint64_t kEndedTaskKeepingTimeSeconds = 3600;  // Todo: Add comment

struct InteractiveTaskAllocationDetail {
  uint32_t node_index;
  std::string ipv4_addr;
  uint32_t port;
  boost::uuids::uuid resource_uuid;
};

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
  Resources res;
};

/**
 * Represent the runtime status on a Xd node.
 * A Node is uniquely identified by (partition id, node index).
 */
struct XdNodeMeta {
  XdNodeStaticMeta static_meta;

  // total = avail + in-use
  Resources res_total;  // A copy of res in XdNodeStaticMeta,
  // just for convenience.
  Resources res_avail;
  Resources res_in_use;

  // Store the information of the slices of allocated resource.
  // One task id owns one shard of allocated resource.
  absl::flat_hash_map<uint32_t /*task id*/, Resources>
      running_task_resource_map;
};

/**
 * A map from index to node information within a partition.
 */
using XdNodeMetaMap = std::unordered_map<uint32_t, XdNodeMeta>;

struct PartitionGlobalMeta {
  // total = avail + in-use
  Resources m_resource_total_;
  Resources m_resource_avail_;
  Resources m_resource_in_use_;

  std::string name;

  // It is used to generate next node index for newly incoming Xd nodes in this
  // partition.
  uint32_t next_node_index = 0;
};

struct PartitionMetas {
  PartitionGlobalMeta partition_global_meta;
  XdNodeMetaMap xd_node_meta_map;
};

struct InteractiveMetaInTask {
  boost::uuids::uuid resource_uuid;
};

struct BatchMetaInTask {
  std::string sh_script;
  std::string output_file_pattern;
};

struct TaskInCtlXd {
  /* -------- Fields that are set at the submission time. ------- */
  absl::Duration time_limit;

  std::string partition_name;
  Resources resources;

  SlurmxGrpc::TaskType type;

  uint32_t node_num{0};
  uint32_t task_per_node{0};

  uid_t uid;

  /* ------- Fields that won't change after this task is accepted. -------- */
  uint32_t task_id;
  uint32_t partition_id;

  /* ----- Fields that may change at run time. ----------- */
  SlurmxGrpc::TaskStatus status;
  bool is_completing{false};

  std::list<uint32_t> node_indexes;
  std::unordered_set<uint32_t> end_node_set;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time start_time;

  absl::Time end_time;

  std::variant<InteractiveMetaInTask, BatchMetaInTask> meta;
};

}  // namespace CtlXd