#include "TaskScheduler.h"

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
  std::list<ISchedulingAlgo::SchedulingResult> sched_result_list;
  while (m_thread_stop_) {
    for (const auto& [task_id, pending_task] : m_pending_task_map_) {
      auto part_metas =
          g_meta_container->GetPartitionMetasPtr(pending_task->partition_id);
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kTaskScheduleIntervalMs));
  }
}

void TaskScheduler::SetSchedulingAlgo(std::unique_ptr<ISchedulingAlgo> algo) {
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

}  // namespace CtlXd