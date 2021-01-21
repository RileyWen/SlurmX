#pragma once

#include <sys/signal.h>
#include <sys/wait.h>

#include <atomic>
#include <forward_list>
#include <optional>
#include <string>
#include <unordered_map>

#include "AnonymousPipe.h"
#include "cgroup.linux.h"

struct TaskInfo {
  std::string name;

  std::string executive_path;
  std::forward_list<std::string> arguments;

  CgroupLimit cg_limit;
};

using TaskInfoRef = std::reference_wrapper<TaskInfo>;
using TaskInfoCRef = std::reference_wrapper<const TaskInfo>;

// Todo: Task may consists of multiple subtasks
struct Task {
  TaskInfo info;
  pid_t root_pid = {};
  std::string cg_path;
};

class TaskManager {
  // TODO: 1. Add SIGCHLD handling
  //       2. Add process group supporting.
 public:
  static TaskManager& GetInstance() {
    static TaskManager singleton;
    return singleton;
  }

  bool AddTask(TaskInfo&& task_info);

  std::optional<TaskInfoCRef> FindTaskInfoByName(const std::string& task_name);

 private:
  static std::string CgroupStrByTask(const Task& task);

  TaskManager() : m_cg_mgr_(CgroupManager::getInstance()) {
    // Only called once. Guaranteed by singleton pattern.
    m_instance_ptr_ = this;

    m_uv_loop_thread_ = std::thread([]() {
      int err;
      if (err > 0) {

      }
    });
  }

  ~TaskManager() { signal(SIGCHLD, SIG_DFL); }

  std::unordered_map<std::string, Task> m_task_map_;
  CgroupManager& m_cg_mgr_;

  // Functions and members for SIGCHLD handling {
  static TaskManager* m_instance_ptr_;

  static void sigchld_handler(int sig);

  struct SigchldInfo {
    pid_t pid;

    enum struct return_type_t { RET_VAL, SIGNAL };
    return_type_t return_type;

    union status_t {
      int ret_val;
      int signum;
    };
    status_t status;
  };
  // }

  std::thread m_uv_loop_thread_;
};