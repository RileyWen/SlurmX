#include "TaskManager.h"

bool TaskManager::AddTask(TaskInfo&& task_info) {
  if (m_task_map_.count(task_info.name) > 0) return false;

  constexpr u_char E_PIPE_OK = 0;
  constexpr u_char E_PIPE_SUICIDE = 1;
  AnonymousPipe anon_pipe;

  pid_t child_pid = fork();
  if (child_pid == 0) {  // Child proc
    anon_pipe.CloseChildEnd();

    // We use u_char here, since the size of u_char is standard-defined.
    u_char val;

    // Blocking read to wait the parent move the child into designated cgroup.
    if (!anon_pipe.ReadIntegerFromParent<u_char>(&val))
      spdlog::error("Failed to write the expected 1 byte to AnonymousPipe.");

    // Use abort to avoid the wrong call to destructor of CgroupManager, which
    // deletes all cgroup in system.
    if (val == E_PIPE_SUICIDE) abort();

    // Release the file descriptor before calling exec()
    anon_pipe.CloseParentEnd();

    std::vector<const char*> argv;
    argv.push_back(task_info.executive_path.c_str());
    for (auto&& arg : task_info.arguments) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    execv(task_info.executive_path.c_str(),
          const_cast<char* const*>(argv.data()));
  } else {  // Parent proc
    u_char pipe_uchar_val;

    anon_pipe.CloseParentEnd();

    Task new_task;
    new_task.root_pid = child_pid;
    new_task.cg_path = CgroupStrByTask(new_task);
    new_task.info = std::move(task_info);

    // Avoid corruption during std::move
    std::string task_name_copy = new_task.info.name;
    if (!m_cg_mgr_.create_or_open(CgroupStrByTask(new_task),
                                  ALL_CONTROLLER_FLAG, NO_CONTROLLER_FLAG,
                                  false)) {
      spdlog::error(
          "Destroy child task process of \"{}\" due to failure of cgroup "
          "creation.",
          new_task.info.name);

      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        spdlog::error("Failed to send E_PIPE_SUICIDE to child.");
    }

    if (!m_cg_mgr_.migrate_proc_to_cgroup(new_task.root_pid,
                                          new_task.cg_path)) {
      spdlog::error(
          "Destroy child task process of \"{}\" due to failure of cgroup "
          "migration.",
          new_task.info.name);

      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        spdlog::error("Failed to send E_PIPE_SUICIDE to child.");
    }

    pipe_uchar_val = E_PIPE_OK;
    if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
      spdlog::error("Failed to send E_PIPE_OK to child.");

    m_task_map_.emplace(std::move(task_name_copy), std::move(new_task));
  }

  return true;
}

std::optional<TaskInfoCRef> TaskManager::FindTaskInfoByName(
    const std::string& task_name) {
  auto iter = m_task_map_.find(task_name);
  if (iter == m_task_map_.end()) return std::nullopt;

  return iter->second.info;
}

std::string TaskManager::CgroupStrByTask(const Task& task) {
  return fmt::format("SlurmX_proc_{}", task.root_pid);
}

void TaskManager::sigchld_handler(int sig) {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      if (WIFEXITED(status)) {
        // Exited with status WEXITSTATUS(status)
        m_instance_ptr_->m_sigchld_queue_.enqueue(
            {pid, SigchldInfo::return_type_t::RET_VAL, {WEXITSTATUS(status)}});
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        m_instance_ptr_->m_sigchld_queue_.enqueue(
            {pid, SigchldInfo::return_type_t::SIGNAL, {WTERMSIG(status)}});
      }
      /* Todo(More status tracing):
       else if (WIFSTOPPED(status)) {
        printf("stopped by signal %d\n", WSTOPSIG(status));
      } else if (WIFCONTINUED(status)) {
        printf("continued\n");
      } */
    } else if (pid == 0)  // There's no child that needs reaping.
      break;
    else if (pid < 0) {
      spdlog::error("waitpid() error: {}", strerror(errno));
      break;
    }
  }
}
