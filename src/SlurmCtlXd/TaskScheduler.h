#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <event2/event.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>

#include "CtlXdPublicDefs.h"
#include "XdNodeMetaContainer.h"
#include "protos/slurmx.pb.h"
#include "slurmx/Lock.h"
#include "slurmx/PublicHeader.h"

namespace CtlXd {

// Task ID is used for querying this structure.
// The node index has also been recorded in XdNodeMetaContainer, so there's no
// need to query it here.
struct QueryBriefTaskMetaFieldControl {
  bool type;
  bool status;
  bool start_time;
};

class INodeSelectionAlgo {
 public:
  // Pair content: <The task which is going to be run,
  //                The node index on which it will be run>
  // Partition information is not needed because scheduling is carried out in
  // one partition to which the task belongs.
  using NodeSelectionResult = std::pair<std::unique_ptr<ITask>, uint32_t>;

  virtual ~INodeSelectionAlgo() = default;

  /**
   * Do node selection for all pending tasks.
   * Note: During this function call, the global meta is locked. This function
   * should return as quick as possible.
   * @param[in,out] all_partitions_meta_map Callee should make necessary
   * modification in this structure to keep the consistency of global meta data.
   * e.g. When a task is added to \b selection_result_list, corresponding
   * resource should subtracted from the fields in all_partitions_meta.
   * @param[in,out] pending_task_map A list that contains all pending task. The
   * list is order by committing time. The later committed task is at the tail
   * of the list. When scheduling is done, scheduled tasks \b SHOULD be removed
   * from \b pending_task_list
   * @param[out] selected_tasks A list that contains the result of
   * scheduling. See the annotation of \b SchedulingResult
   */
  virtual void NodeSelect(
      const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      const absl::flat_hash_map<uint32_t, std::unique_ptr<ITask>>&
          running_tasks,
      absl::btree_map<uint32_t, std::unique_ptr<ITask>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) = 0;
};

class MinLoadFirst : public INodeSelectionAlgo {
 public:
  void NodeSelect(
      const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      const absl::flat_hash_map<uint32_t, std::unique_ptr<ITask>>&
          running_tasks,
      absl::btree_map<uint32_t, std::unique_ptr<ITask>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) override;
};

class TaskScheduler {
  using Mutex = absl::Mutex;
  using LockGuard = util::AbslMutexLockGuard;

  template <typename K, typename V>
  using HashMap = absl::flat_hash_map<K, V>;

  template <typename K>
  using HashSet = absl::flat_hash_set<K>;

 public:
  explicit TaskScheduler(std::unique_ptr<INodeSelectionAlgo> algo);

  ~TaskScheduler();

  void SetNodeSelectionAlgo(std::unique_ptr<INodeSelectionAlgo> algo);

  SlurmxErr SubmitTask(std::unique_ptr<ITask> task, uint32_t* task_id);

  void TaskStatusChange(uint32_t task_id, ITask::Status new_status,
                        std::optional<std::string> reason);

  // Temporary inconsistency may happen. If 'false' is returned, just ignore it.
  void QueryTaskBriefMetaInPartition(
      uint32_t partition_id,
      const QueryBriefTaskMetaFieldControl& field_control,
      google::protobuf::RepeatedPtrField<SlurmxGrpc::BriefTaskMeta>*
          task_metas);

  bool QueryXdNodeIdOfRunningTask(uint32_t task_id, XdNodeId* node_id);

 private:
  void ScheduleThread_();

  void CleanEndedTaskThread_();

  std::unique_ptr<INodeSelectionAlgo> m_node_selection_algo_;

  boost::uuids::random_generator_mt19937 m_uuid_gen_;
  uint32_t m_next_task_id_;

  // Ordered by task id. Those who comes earlier are in the head,
  // Because they have smaller task id.
  absl::btree_map<uint32_t /*Task Id*/, std::unique_ptr<ITask>>
      m_pending_task_map_ GUARDED_BY(m_pending_task_map_mtx_);
  Mutex m_pending_task_map_mtx_;

  absl::flat_hash_map<uint32_t, std::unique_ptr<ITask>> m_running_task_map_
      GUARDED_BY(m_running_task_map_mtx_);
  Mutex m_running_task_map_mtx_;

  absl::flat_hash_map<uint32_t, std::unique_ptr<ITask>> m_ended_task_map_
      GUARDED_BY(m_ended_task_map_mtx_);
  Mutex m_ended_task_map_mtx_;

  // Task Indexes
  HashMap<uint32_t /* Node Index */, HashSet<uint32_t /* Task ID*/>>
      m_node_to_tasks_map_ GUARDED_BY(m_task_indexes_mtx_);
  HashMap<uint32_t /* Partition ID */, HashSet<uint32_t /* Task ID */>>
      m_partition_to_tasks_map_ GUARDED_BY(m_task_indexes_mtx_);
  Mutex m_task_indexes_mtx_;

  std::thread m_schedule_thread_;
  std::thread m_clean_ended_thread_;
  std::atomic_bool m_thread_stop_{};
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::TaskScheduler> g_task_scheduler;