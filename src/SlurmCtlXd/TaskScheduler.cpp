#include "TaskScheduler.h"

#include <algorithm>

#include "slurmx/String.h"

namespace CtlXd {

TaskScheduler::TaskScheduler() {
  m_schedule_thread_ =
      std::thread(std::bind(&TaskScheduler::ScheduleThread_, this));
}

TaskScheduler::~TaskScheduler() {
  m_thread_stop_ = true;
  m_schedule_thread_.join();
}

void TaskScheduler::ScheduleThread_() {
  std::list<INodeSelectionAlgo::NodeSelectionResult> sched_result_list;
  while (m_thread_stop_) {
    for (const auto& [task_id, pending_task] : m_pending_task_map_) {
      auto part_metas =
          g_meta_container->GetPartitionMetasPtr(pending_task->partition_id);
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kTaskScheduleIntervalMs));
  }
}

void TaskScheduler::SetSchedulingAlgo(
    std::unique_ptr<INodeSelectionAlgo> algo) {
  m_sched_algo_ = std::move(algo);
}

SlurmxErr TaskScheduler::SubmitTask(std::unique_ptr<ITask> task,
                                    BasicTaskMeta* task_meta) {
  // Check whether the selected partition exists.
  if (!g_meta_container->PartitionExists(task->partition_name))
    return SlurmxErr::kNonExistent;

  uint32_t partition_id =
      g_meta_container->GetPartitionId(task->partition_name);

  // Check whether the selected partition is able to run this task.
  auto metas_ptr = g_meta_container->GetPartitionMetasPtr(partition_id);
  if (metas_ptr->partition_global_meta.m_resource_avail_ < task->resources) {
    SLURMX_TRACE("Resource not enough. Avail: cpu {}, mem: {}, mem+sw: {}",
                 metas_ptr->partition_global_meta.m_resource_avail_
                     .allocatable_resource.cpu_count,
                 slurmx::ReadableMemory(
                     metas_ptr->partition_global_meta.m_resource_avail_
                         .allocatable_resource.memory_bytes),
                 slurmx::ReadableMemory(
                     metas_ptr->partition_global_meta.m_resource_avail_
                         .allocatable_resource.memory_sw_bytes));
    return SlurmxErr::kNoResource;
  }

  // Add the task to the pending task queue.
  task->task_id = m_next_task_id_++;
  task->partition_id = partition_id;
  task->status = ITask::Status::Pending;

  m_pending_task_map_mtx_.lock();
  m_pending_task_map_.emplace(task->task_id, std::move(task));
  m_pending_task_map_mtx_.unlock();

  return SlurmxErr::kOk;
}

void MinLoadFirst::NodeSelect(
    const XdNodeMetaContainerInterface::AllPartitionsMetaMap&
        all_partitions_meta_map,
    absl::btree_map<uint32_t, std::unique_ptr<ITask>>* pending_task_map,
    std::list<NodeSelectionResult>* scheduling_result_list) {
  /**
   * In this map, the time is discretized by 1s and starts from absl::Now().
   * {x: a, y: b, z: c, ...} means that
   * In time interval [x, y-1], the amount of available resources is a.
   * In time interval [y, z-1], the amount of available resources is b.
   * In time interval [z, ...], the amount of available resources is c.
   */
  using TimeAvailResMap = absl::btree_map<absl::Time, Resources>;

  struct NodeSelectionInfo {
    absl::btree_multimap<uint32_t /* # of running tasks */,
                         uint32_t /* node index */>
        task_num__node_id__map;
    absl::flat_hash_map<uint32_t /* Node Index*/, TimeAvailResMap>
        node__time__avail_res__map;
  };
  absl::flat_hash_map<uint32_t /* Partition Id */, NodeSelectionInfo>
      part_id__node_info__map;

  absl::Time now = absl::Now();

  for (auto& [partition_id, partition_metas] : all_partitions_meta_map) {
    for (auto& [node_index, node_meta] : partition_metas.xd_node_meta_map) {
      NodeSelectionInfo& node_info_in_a_partition =
          part_id__node_info__map[partition_id];

      node_info_in_a_partition.task_num__node_id__map.emplace(
          node_meta.running_tasks.size(), node_index);

      // Sort all task in this node by ending time.
      std::vector<std::pair<absl::Time, uint32_t>> end_time__task_id_vec;
      for (const auto& [task_id, running_task] : node_meta.running_tasks) {
        end_time__task_id_vec.push_back(
            {running_task->start_time + running_task->time_limit, task_id});
      }
      std::sort(end_time__task_id_vec.begin(), end_time__task_id_vec.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.first < rhs.first;
                });

      // Calculate how many resources are available at [now, first task end,
      //  second task end, ...] in this node.
      auto& time__avail_res__map =
          node_info_in_a_partition.node__time__avail_res__map[node_index];
      time__avail_res__map[now] = node_meta.res_avail;

      {  // Limit the scope of `iter`
        auto iter = time__avail_res__map.find(now);
        for (auto& [end_time, task_id] : end_time__task_id_vec) {
          const auto& running_task = node_meta.running_tasks.at(task_id);
          if (!time__avail_res__map.contains(end_time + absl::Seconds(1))) {
            /**
             * For the situation in which multiple tasks may end at the same
             * time:
             * end_time__task_id_vec: [{now+1, 1}, {now+1, 2}, ...]
             * But we want only 1 time point in time__avail_res__map:
             * {{now+1+1: available_res(now) + available_res(1) +
             *  available_res(2)}, ...}
             */
            time__avail_res__map[end_time + absl::Seconds(1)] = iter->second;
            iter++;
          }

          time__avail_res__map[end_time + absl::Seconds(1)] +=
              running_task->resources;
        }
      }
    }
  }

  // Now we know the # of running tasks and how many resources are available at
  //  the end of each task, on each node in all partitions.
  // Iterate over all the pending tasks and select the available node for the
  //  task to run in its partition.
  for (auto it = pending_task_map->begin(); it != pending_task_map->end();) {
    uint32_t task_id = it->first;
    auto& task = it->second;
    NodeSelectionInfo& node_info = part_id__node_info__map[task->partition_id];

    uint32_t min_load_node_index =
        node_info.task_num__node_id__map.begin()->second;
    TimeAvailResMap& time__avail_res__map =
        node_info.node__time__avail_res__map[min_load_node_index];

    // Find expected start time in this node for this task.
    // The expected start time must exist because all tasks in pending_task_map
    // can be run under the amount of all resources in this node. At some future
    // time point, all tasks will end and this pending task can eventually be
    // run.
    absl::Time expected_start_time;
    TimeAvailResMap::iterator task_duration_begin_it;
    TimeAvailResMap::iterator task_duration_end_it;
    for (task_duration_begin_it = time__avail_res__map.begin();
         task_duration_begin_it != time__avail_res__map.end();
         task_duration_begin_it++) {
      absl::Time end_time = task_duration_begin_it->first + task->time_limit;

      bool resource_enough_in_this_duration = true;
      task_duration_end_it = time__avail_res__map.upper_bound(end_time);
      for (auto in_duration_it = task_duration_begin_it;
           in_duration_it != task_duration_end_it; in_duration_it++)
        if (!(task->resources < in_duration_it->second)) {
          // Some kind of resources exceeds the current available amount.
          resource_enough_in_this_duration = false;
          break;
        }

      if (resource_enough_in_this_duration) {
        expected_start_time = task_duration_begin_it->first;
        task->start_time = expected_start_time;
        break;
      }
    }

    absl::Time task_end_time_plus_1s =
        expected_start_time + task->time_limit + absl::Seconds(1);
    if (!time__avail_res__map.contains(task_end_time_plus_1s)) {
      // Assume one task end at time x-2,
      // If "x-2" lies in the interval [x, y-1) in time__avail_res__map,
      //  for example, x+2 in [x, y-1) with the available resources amount a,
      //  we need to divide this interval into to two intervals:
      //  [x, x+2]: a-k, where k is the resource amount that task requires,
      //  [x+3, y-1]: a
      // Therefore, we need to insert a key-value at x+3 to preserve this.
      // However, if the length of [x+3, y-1] is 0, or more simply, the point
      //  x+3 exists, there's no need to save the interval [x+3, y-1].
      auto last_elem_it_in_duration = task_duration_end_it;
      last_elem_it_in_duration--;
      time__avail_res__map.emplace(task_end_time_plus_1s,
                                   last_elem_it_in_duration->second);
    }

    // Subtract the required resources within the interval.
    for (auto in_duration_it = task_duration_begin_it;
         in_duration_it != task_duration_end_it; in_duration_it++) {
      in_duration_it->second -= task->resources;
    }

    if (expected_start_time == now) {
      // The task can be started now.

      std::unique_ptr<ITask> moved_task;

      // Move task out of pending_task_map and insert it to the
      // scheduling_result_list.
      moved_task.swap(task);
      scheduling_result_list->emplace_back(std::move(moved_task),
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