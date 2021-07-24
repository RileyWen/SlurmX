#pragma once

#include <absl/container/btree_map.h>

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

struct ITask {
  enum class Type { Interactive, Batch };
  enum class Status { Pending, Scheduled, Running, Finished, Abort };

  /* -------- Fields that are set at the submission time. ------- */
  uint64_t time_limit_sec;

  std::string partition_name;
  Resources resources;

  Type type;

  /* ------- Fields that won't change after this task is accepted. -------- */
  uint32_t task_id;
  uint32_t partition_id;

  /* ----- Fields that may change at run time. ----------- */
  Status status;
  uint64_t estimated_start_time;

  virtual ~ITask() = default;

 protected:
  ITask() = default;
};

struct InteractiveTask : public ITask {
  using ITask::ITask;
};

struct BatchTask : public ITask {
  using ITask::ITask;
};

struct BasicTaskMeta {
  uint32_t task_id;
  boost::uuids::uuid resource_uuid;
};

struct InteractiveTaskAllocationDetail {
  uint32_t node_index;
  std::string ipv4_addr;
  uint32_t port;
};

class ISchedulingAlgo {
 public:
  // Pair content: <The task which is going to be run,
  //                The node index on which it will be run>
  // Partition information is not needed because scheduling is carried out in
  // one partition to which the task belongs.
  using SchedulingResult = std::pair<std::unique_ptr<ITask>, uint32_t>;

  virtual ~ISchedulingAlgo() = default;

  /**
   * Do actual scheduling.
   * Note: During this function call, the global meta is locked. This function
   * should return as quick as possible.
   * @param[in] partition_metas The reference to the task's partition meta.
   * @param[in,out] pending_task_list A list that contains all pending task. The
   * list is order by committing time. The later committed task is at the tail
   * of the list. When scheduling is done, scheduled tasks \b SHOULD be removed
   * from \b pending_task_list
   * @param[out] scheduling_result_list A list that contains the result of
   * scheduling. See the annotation of \b SchedulingResult
   */
  virtual void Schedule(
      const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      std::list<std::unique_ptr<ITask>>* pending_task_list,
      std::list<SchedulingResult>* scheduling_result_list) = 0;
};

class TaskScheduler {
  using Mutex = slurmx::mutex;
  using LockGuard = slurmx::lock_guard;

 public:
  TaskScheduler();

  ~TaskScheduler();

  void SetSchedulingAlgo(std::unique_ptr<ISchedulingAlgo> algo);

  SlurmxErr SubmitTask(std::unique_ptr<ITask> task, BasicTaskMeta* task_meta);

 private:
  void ScheduleThread_();

  std::unique_ptr<ISchedulingAlgo> m_sched_algo_;

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