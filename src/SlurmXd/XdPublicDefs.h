#pragma once

#include "slurmx/PublicHeader.h"

namespace Xd {

struct TaskStatusChange {
  uint32_t task_id;
  ITask::Status new_status;
  std::optional<std::string> reason;
};

}  // namespace Xd