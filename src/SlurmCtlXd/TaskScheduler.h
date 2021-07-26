#pragma once

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>

#include "CtlXdPublicDefs.h"
#include "PublicHeader.h"
#include "XdNodeMetaContainer.h"
#include "slurmx/Lock.h"

namespace CtlXd {

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
   * @param[in] partition_metas The reference to the task's partition meta.
   * @param[in,out] pending_task_map A list that contains all pending task. The
   * list is order by committing time. The later committed task is at the tail
   * of the list. When scheduling is done, scheduled tasks \b SHOULD be removed
   * from \b pending_task_list
   * @param[out] selection_result_list A list that contains the result of
   * scheduling. See the annotation of \b SchedulingResult
   */
  virtual void NodeSelect(
      const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      absl::btree_map<uint32_t, std::unique_ptr<ITask>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) = 0;
};

class MinLoadFirst : public INodeSelectionAlgo {
 public:
  void NodeSelect(
      const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      absl::btree_map<uint32_t, std::unique_ptr<ITask>>* pending_task_map,
      std::list<NodeSelectionResult>* scheduling_result_list) override;
};

class TaskScheduler {
  using Mutex = slurmx::mutex;
  using LockGuard = slurmx::lock_guard;

 public:
  TaskScheduler();

  ~TaskScheduler();

  void SetSchedulingAlgo(std::unique_ptr<INodeSelectionAlgo> algo);

  SlurmxErr SubmitTask(std::unique_ptr<ITask> task, BasicTaskMeta* task_meta);

 private:
  void ScheduleThread_();

  std::unique_ptr<INodeSelectionAlgo> m_sched_algo_;

  boost::uuids::random_generator_mt19937 m_uuid_gen_;
  uint32_t m_next_task_id_;

  // Ordered by task id. Those who comes earlier are in the head,
  // Because they have smaller task id.
  absl::btree_map<uint32_t /*Task Id*/, std::unique_ptr<ITask>>
      m_pending_task_map_;
  Mutex m_pending_task_map_mtx_;

  std::thread m_schedule_thread_;
  std::atomic_bool m_thread_stop_;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::TaskScheduler> g_task_scheduler;