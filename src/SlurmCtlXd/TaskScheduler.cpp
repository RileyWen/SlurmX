#include "TaskScheduler.h"

#include <algorithm>
#include <map>

#include "CtlXdGrpcServer.h"
#include "XdNodeKeeper.h"
#include "slurmx/String.h"

namespace CtlXd {

TaskScheduler::TaskScheduler(std::unique_ptr<INodeSelectionAlgo> algo)
    : m_next_task_id_(0), m_node_selection_algo_(std::move(algo)) {
  m_schedule_thread_ = std::thread([this] { ScheduleThread_(); });

  m_clean_ended_thread_ = std::thread([this] { CleanEndedTaskThread_(); });
}

TaskScheduler::~TaskScheduler() {
  m_thread_stop_ = true;
  m_schedule_thread_.join();
  m_clean_ended_thread_.join();
}

void TaskScheduler::ScheduleThread_() {
  while (!m_thread_stop_) {
    m_pending_task_map_mtx_.Lock();
    if (!m_pending_task_map_.empty()) {  // all_part_metas is locked here.
      auto all_part_metas = g_meta_container->GetAllPartitionsMetaMapPtr();
      std::list<INodeSelectionAlgo::NodeSelectionResult> selection_result_list;

      // Both read and write need a lock.
      m_running_task_map_mtx_.Lock();
      m_node_selection_algo_->NodeSelect(*all_part_metas, m_running_task_map_,
                                         &m_pending_task_map_,
                                         &selection_result_list);
      m_running_task_map_mtx_.Unlock();
      m_pending_task_map_mtx_.Unlock();

      for (auto& it : selection_result_list) {
        auto& task = it.first;
        uint32_t partition_id = task->partition_id;
        uint32_t node_index = it.second;

        XdNodeId node_id{partition_id, node_index};
        g_meta_container->MallocResourceFromNode(node_id, task->task_id,
                                                 task->resources);

        if (task->type == ITask::Type::Interactive) {
          XdNodeMeta node_meta =
              all_part_metas->at(partition_id).xd_node_meta_map.at(node_index);
          InteractiveTaskAllocationDetail detail{
              .node_index = node_index,
              .ipv4_addr = node_meta.static_meta.ipv4_addr,
              .port = node_meta.static_meta.port,
              .resource_uuid = m_uuid_gen_(),
          };

          dynamic_cast<InteractiveTask*>(task.get())->resource_uuid =
              detail.resource_uuid;

          g_ctlxd_server->AddAllocDetailToIaTask(task->task_id,
                                                 std::move(detail));
        }

        const auto* task_ptr = task.get();

        m_running_task_map_mtx_.Lock();
        m_running_task_map_.emplace(task->task_id, std::move(task));
        m_running_task_map_mtx_.Unlock();

        XdNodeStub* node_stub = g_node_keeper->GetXdStub(node_id);
        node_stub->ExecuteTask(task_ptr);
      }
      // all_part_metas is unlocked here.
    } else {
      m_pending_task_map_mtx_.Unlock();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kTaskScheduleIntervalMs));
  }
}

void TaskScheduler::CleanEndedTaskThread_() {
  while (!m_thread_stop_) {
    {
      LockGuard guard(m_ended_task_map_mtx_);
      for (auto iter = m_ended_task_map_.begin();
           iter != m_ended_task_map_.end();) {
        if (iter->second->end_time +
                absl::Seconds(kEndedTaskKeepingTimeSeconds) >
            absl::Now()) {
          auto copy_iter = iter++;
          m_ended_task_map_.erase(copy_iter);
        } else
          iter++;
      }
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(kEndedTaskCleanIntervalSeconds));
  }
}

void TaskScheduler::SetNodeSelectionAlgo(
    std::unique_ptr<INodeSelectionAlgo> algo) {
  m_node_selection_algo_ = std::move(algo);
}

SlurmxErr TaskScheduler::SubmitTask(std::unique_ptr<ITask> task,
                                    uint32_t* task_id) {
  // Check whether the selected partition exists.
  if (!g_meta_container->PartitionExists(task->partition_name))
    return SlurmxErr::kNonExistent;

  uint32_t partition_id =
      g_meta_container->GetPartitionId(task->partition_name);

  // Check whether the selected partition is able to run this task.
  auto metas_ptr = g_meta_container->GetPartitionMetasPtr(partition_id);
  if (metas_ptr->partition_global_meta.m_resource_avail_ < task->resources) {
    SLURMX_TRACE(
        "Resource not enough. Avail: cpu {}, mem: {}, mem+sw: {}",
        metas_ptr->partition_global_meta.m_resource_avail_.allocatable_resource
            .cpu_count,
        util::ReadableMemory(metas_ptr->partition_global_meta.m_resource_avail_
                                 .allocatable_resource.memory_bytes),
        util::ReadableMemory(metas_ptr->partition_global_meta.m_resource_avail_
                                 .allocatable_resource.memory_sw_bytes));
    return SlurmxErr::kNoResource;
  }

  // Add the task to the pending task queue.
  task->task_id = m_next_task_id_++;
  task->partition_id = partition_id;
  task->status = ITask::Status::Pending;

  *task_id = task->task_id;

  m_pending_task_map_mtx_.Lock();
  m_pending_task_map_.emplace(task->task_id, std::move(task));
  m_pending_task_map_mtx_.Unlock();

  return SlurmxErr::kOk;
}

void TaskScheduler::TaskStatusChange(uint32_t task_id, ITask::Status new_status,
                                     std::optional<std::string> reason) {
  LockGuard running_guard(m_running_task_map_mtx_);
  LockGuard ended_guard(m_ended_task_map_mtx_);

  auto iter = m_running_task_map_.find(task_id);
  if (iter == m_running_task_map_.end()) {
    SLURMX_ERROR("Unknown task id {} when change task status.", task_id);
    return;
  }

  std::unique_ptr<ITask> task;
  task = std::move(iter->second);
  m_running_task_map_.erase(iter);

  SLURMX_DEBUG("TaskStatusChange: Task #{} {}->{}", task_id, task->status,
               new_status);
  task->status = new_status;

  if (task->type == ITask::Type::Interactive) {
    g_ctlxd_server->RemoveAllocDetailOfIaTask(task_id);
  }

  g_meta_container->FreeResourceFromNode({task->partition_id, task->node_index},
                                         task_id);
  m_ended_task_map_.emplace(task_id, std::move(task));
}

bool TaskScheduler::QueryXdNodeIdOfRunningTask(uint32_t task_id,
                                               XdNodeId* xd_node_id) {
  LockGuard running_guard(m_running_task_map_mtx_);

  auto iter = m_running_task_map_.find(task_id);
  if (iter == m_running_task_map_.end()) {
    SLURMX_ERROR("Unknown task id {} when change task status.", task_id);
    return false;
  }
  xd_node_id->partition_id = iter->second->partition_id;
  xd_node_id->node_index = iter->second->node_index;
  return true;
}

void MinLoadFirst::NodeSelect(
    const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
        all_partitions_meta_map,
    const absl::flat_hash_map<uint32_t, std::unique_ptr<ITask>>& running_tasks,
    absl::btree_map<uint32_t, std::unique_ptr<ITask>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  /**
   * In this map, the time is discretized by 1s and starts from absl::Now().
   * {x: a, y: b, z: c, ...} means that
   * In time interval [x, y-1], the amount of available resources is a.
   * In time interval [y, z-1], the amount of available resources is b.
   * In time interval [z, ...], the amount of available resources is c.
   */
  using TimeAvailResMap = std::map<absl::Time, Resources>;

  struct NodeSelectionInfo {
    absl::btree_multimap<uint32_t /* # of running tasks */,
                         uint32_t /* node index */>
        task_num_node_id_map;
    absl::flat_hash_map<uint32_t /* Node Index*/, TimeAvailResMap>
        node_time_avail_res_map;
  };
  absl::flat_hash_map<uint32_t /* Partition ID */, NodeSelectionInfo>
      part_id_node_info_map;

  // Truncated by 1s
  absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

  for (auto& [partition_id, partition_metas] : all_partitions_meta_map) {
    for (auto& [node_index, node_meta] : partition_metas.xd_node_meta_map) {
      NodeSelectionInfo& node_info_in_a_partition =
          part_id_node_info_map[partition_id];

      node_info_in_a_partition.task_num_node_id_map.emplace(
          node_meta.running_task_resource_map.size(), node_index);

      // Sort all running task in this node by ending time.
      std::vector<std::pair<absl::Time, uint32_t>> end_time_task_id_vec;
      for (const auto& [task_id, running_task] : running_tasks) {
        if (running_task->partition_id == partition_id &&
            running_task->node_index == node_index)
          end_time_task_id_vec.emplace_back(
              running_task->start_time + running_task->time_limit, task_id);
      }
      std::sort(end_time_task_id_vec.begin(), end_time_task_id_vec.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.first < rhs.first;
                });

#ifndef NDEBUG
      {
        if (!end_time_task_id_vec.empty()) {
          std::string str;
          str.append(fmt::format("Node ({}, {}): ", partition_id, node_index));
          for (auto [end_time, task_id] : end_time_task_id_vec) {
            str.append(fmt::format("Task #{} ends after {}s", task_id,
                                   absl::ToInt64Seconds(end_time - now)));
          }
          SLURMX_TRACE("{}", str);
        }
      }
#endif

      // Calculate how many resources are available at [now, first task end,
      //  second task end, ...] in this node.
      auto& time_avail_res_map =
          node_info_in_a_partition.node_time_avail_res_map[node_index];
      time_avail_res_map[now] = node_meta.res_avail;

      {  // Limit the scope of `iter`
        auto prev_time_iter = time_avail_res_map.find(now);
        bool ok;
        for (auto& [end_time, task_id] : end_time_task_id_vec) {
          const auto& running_task = running_tasks.at(task_id);
          if (time_avail_res_map.count(end_time + absl::Seconds(1)) == 0) {
            /**
             * For the situation in which multiple tasks may end at the same
             * time:
             * end_time__task_id_vec: [{now+1, 1}, {now+1, 2}, ...]
             * But we want only 1 time point in time__avail_res__map:
             * {{now+1+1: available_res(now) + available_res(1) +
             *  available_res(2)}, ...}
             */
            std::tie(prev_time_iter, ok) = time_avail_res_map.emplace(
                end_time + absl::Seconds(1), prev_time_iter->second);
          }

          time_avail_res_map[end_time + absl::Seconds(1)] +=
              running_task->resources;
        }
#ifndef NDEBUG
        {
          std::string str;
          str.append(fmt::format("Node ({}, {}): ", partition_id, node_index));
          auto prev_iter = time_avail_res_map.begin();
          auto iter = std::next(prev_iter);
          for (; iter != time_avail_res_map.end(); prev_iter++, iter++) {
            str.append(fmt::format(
                "[ now+{}s , now+{}s ) Available allocatable "
                "res: cpu core {}, mem {}",
                absl::ToInt64Seconds(prev_iter->first - now),
                absl::ToInt64Seconds(iter->first - now),
                prev_iter->second.allocatable_resource.cpu_count,
                prev_iter->second.allocatable_resource.memory_bytes));
          }
          str.append(
              fmt::format("[ now+{}s , inf ) Available allocatable "
                          "res: cpu core {}, mem {}",
                          absl::ToInt64Seconds(prev_iter->first - now),
                          prev_iter->second.allocatable_resource.cpu_count,
                          prev_iter->second.allocatable_resource.memory_bytes));
          SLURMX_TRACE("{}", str);
        }
#endif
      }
    }
  }

  // Now we know, on each node in all partitions, the # of running tasks (which
  //  doesn't include those we select as the incoming running tasks in the
  //  following code) and how many resources are available at the end of each
  //  task.
  // Iterate over all the pending tasks and select the available node for the
  //  task to run in its partition.
  for (auto it = pending_task_map->begin(); it != pending_task_map->end();) {
    uint32_t task_id = it->first;
    auto& task = it->second;
    NodeSelectionInfo& node_info = part_id_node_info_map[task->partition_id];

    uint32_t min_load_node_index =
        node_info.task_num_node_id_map.begin()->second;
    TimeAvailResMap& time_avail_res_map =
        node_info.node_time_avail_res_map[min_load_node_index];

    // Find expected start time in this node for this task.
    // The expected start time must exist because all tasks in pending_task_map
    // can be run under the amount of all resources in this node. At some future
    // time point, all tasks will end and this pending task can eventually be
    // run.
    absl::Time expected_start_time;
    TimeAvailResMap::iterator task_duration_begin_it;
    TimeAvailResMap::iterator task_duration_end_it;
    for (task_duration_begin_it = time_avail_res_map.begin();
         task_duration_begin_it != time_avail_res_map.end();
         task_duration_begin_it++) {
      absl::Time end_time = task_duration_begin_it->first + task->time_limit;

      bool resource_enough_in_this_duration = true;
      task_duration_end_it = time_avail_res_map.upper_bound(end_time);

#ifndef NDEBUG
      if (task_duration_end_it == time_avail_res_map.end())
        SLURMX_TRACE("\t Trying duration [ now+{}s , inf ) for task #{}",
                     absl::ToInt64Seconds(task_duration_begin_it->first - now),
                     task_id);
      else
        SLURMX_TRACE("\t Trying duration [ now+{}s , now+{}s ) for task #{}",
                     absl::ToInt64Seconds(task_duration_begin_it->first - now),
                     absl::ToInt64Seconds(task_duration_end_it->first - now),
                     task_id);

#endif
      for (auto in_duration_it = task_duration_begin_it;
           in_duration_it != task_duration_end_it; in_duration_it++)
        if (!(task->resources <= in_duration_it->second)) {
          // Some kind of resources exceeds the current available amount.
          resource_enough_in_this_duration = false;
          break;
        }

      if (resource_enough_in_this_duration) {
        expected_start_time = task_duration_begin_it->first;
        break;
      }
    }

    absl::Time task_end_time_plus_1s =
        expected_start_time + task->time_limit + absl::Seconds(1);
    if (time_avail_res_map.count(task_end_time_plus_1s) == 0) {
      // Assume one task end at time x-2,
      // If "x-2" lies in the interval [x, y-1) in time__avail_res__map,
      //  for example, x+2 in [x, y-1) with the available resources amount a,
      //  we need to divide this interval into to two intervals:
      //  [x, x+2]: a-k, where k is the resource amount that task requires,
      //  [x+3, y-1]: a
      // Therefore, we need to insert a key-value at x+3 to preserve this.
      // However, if the length of [x+3, y-1] is 0, or more simply, the point
      //  x+3 exists, there's no need to save the interval [x+3, y-1].
      auto last_elem_it_in_duration = std::prev(task_duration_end_it);
#ifndef NDEBUG
      if (task_duration_end_it == time_avail_res_map.end())
        SLURMX_TRACE(
            "\t Insert duration [ now+{}s , inf ) : cpu: {} for task #{}",
            absl::ToInt64Seconds(task_end_time_plus_1s - now),
            last_elem_it_in_duration->second.allocatable_resource.cpu_count,
            task_id);
      else
        SLURMX_TRACE(
            "\t Insert duration [ now+{}s , now+{}s ) : cpu: {} for task #{}",
            absl::ToInt64Seconds(task_end_time_plus_1s - now),
            absl::ToInt64Seconds(task_duration_end_it->first - now),
            last_elem_it_in_duration->second.allocatable_resource.cpu_count,
            task_id);
#endif
      bool ok;
      std::tie(task_duration_end_it, ok) = time_avail_res_map.emplace(
          task_end_time_plus_1s, last_elem_it_in_duration->second);
      SLURMX_ASSERT(ok == true, "Insertion must be successful.");
    }

    // Subtract the required resources within the interval.
    for (auto in_duration_it = task_duration_begin_it;
         in_duration_it != task_duration_end_it; in_duration_it++) {
      in_duration_it->second -= task->resources;
    }

#ifndef NDEBUG
    {
      std::string str{"\n"};
      str.append(fmt::format("Node ({}, {}): \n", task->partition_id,
                             task->node_index));
      auto prev_iter = time_avail_res_map.begin();
      auto iter = std::next(prev_iter);
      for (; iter != time_avail_res_map.end(); prev_iter++, iter++) {
        str.append(
            fmt::format("\t[ now+{}s , now+{}s ) Available allocatable "
                        "res: cpu core {}, mem {}\n",
                        absl::ToInt64Seconds(prev_iter->first - now),
                        absl::ToInt64Seconds(iter->first - now),
                        prev_iter->second.allocatable_resource.cpu_count,
                        prev_iter->second.allocatable_resource.memory_bytes));
      }
      str.append(
          fmt::format("\t[ now+{}s , inf ) Available allocatable "
                      "res: cpu core {}, mem {}\n",
                      absl::ToInt64Seconds(prev_iter->first - now),
                      prev_iter->second.allocatable_resource.cpu_count,
                      prev_iter->second.allocatable_resource.memory_bytes));
      SLURMX_TRACE("{}", str);
    }
#endif

    if (expected_start_time == now) {
      // The task can be started now.
      task->start_time = expected_start_time;

      // Increase the running task num in the local variable.
      // We leave the change in all_partitions_meta_map and running_tasks to
      // the scheduling thread caller to avoid lock maintenance and deadlock.
      uint32_t task_num_in_this_node =
          node_info.task_num_node_id_map.begin()->first;
      node_info.task_num_node_id_map.erase(
          node_info.task_num_node_id_map.begin());
      node_info.task_num_node_id_map.emplace(task_num_in_this_node + 1,
                                             min_load_node_index);

      std::unique_ptr<ITask> moved_task;

      // Move task out of pending_task_map and insert it to the
      // scheduling_result_list.
      moved_task.swap(task);
      selection_result_list->emplace_back(std::move(moved_task),
                                          min_load_node_index);

      // Erase the empty task unique_ptr from map and move to the next element
      it = pending_task_map->erase(it);
    } else {
      // The task can't be started now. Move to the next pending task.
      it++;
    }
  }
}

}  // namespace CtlXd