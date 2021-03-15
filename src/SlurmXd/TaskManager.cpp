#include "TaskManager.h"

TaskManager::TaskManager()
    : m_cg_mgr_(Cgroup::CgroupManager::getInstance()),
      m_ev_sigchld_(nullptr),
      m_ev_base_(nullptr),
      m_ev_grpc_event_(nullptr),
      m_is_ending_now_(false) {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_ev_base_ = event_base_new();
  if (!m_ev_base_) {
    SLURMX_ERROR("Could not initialize libevent!");
    abort();
  }

  m_ev_sigchld_ = evsignal_new(m_ev_base_, SIGCHLD, ev_sigchld_cb_, this);
  if (!m_ev_sigchld_) {
    SLURMX_ERROR("Failed to create the SIGCHLD event!");
    abort();
  }

  if (event_add(m_ev_sigchld_, nullptr) < 0) {
    SLURMX_ERROR("Could not add the SIGCHLD event to base!");
    abort();
  }

  m_ev_sigint_ = evsignal_new(m_ev_base_, SIGINT, ev_sigint_cb_, this);
  if (!m_ev_sigint_) {
    SLURMX_ERROR("Failed to create the SIGCHLD event!");
    abort();
  }

  if (event_add(m_ev_sigint_, nullptr) < 0) {
    SLURMX_ERROR("Could not add the SIGINT event to base!");
    abort();
  }

  if ((m_grpc_event_fd_ = eventfd(0, EFD_SEMAPHORE)) < 0) {
    SLURMX_ERROR("Failed to init the eventfd!");
    abort();
  }

  m_ev_grpc_event_ = event_new(m_ev_base_, m_grpc_event_fd_,
                               EV_PERSIST | EV_READ, ev_grpc_event_cb_, this);
  if (!m_ev_grpc_event_) {
    SLURMX_ERROR("Failed to create the grpc event!");
    abort();
  }

  if (event_add(m_ev_grpc_event_, nullptr) < 0) {
    SLURMX_ERROR("Could not add the grpc event to base!");
    abort();
  }

  m_ev_loop_thread_ =
      std::thread([this]() { event_base_dispatch(m_ev_base_); });
}

TaskManager::~TaskManager() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();

  if (m_ev_sigchld_) event_free(m_ev_sigchld_);
  if (m_ev_sigint_) event_free(m_ev_sigint_);
  if (m_ev_grpc_event_) event_free(m_ev_grpc_event_);
  if (m_ev_base_) event_base_free(m_ev_base_);
}

SlurmxErr TaskManager::AddTaskAsync(TaskInitInfo&& task_init_info) {
  if (m_is_ending_now_) {
    return SlurmxErr::kStop;
  } else {
    eventfd_t u = 1;

    std::promise<grpc_resp_new_task_t> resp_prom;
    std::future<grpc_resp_new_task_t> resp_future = resp_prom.get_future();

    grpc_req_new_task_t req{
        std::move(task_init_info),
        std::move(resp_prom),
    };

    m_gprc_new_task_queue_.enqueue(std::move(req));
    ssize_t s = eventfd_write(m_grpc_event_fd_, u);
    if (s < 0) {
      SLURMX_ERROR("Failed to write to grpc event fd: {}", strerror(errno));
      return SlurmxErr::kSystemErr;
    }

    grpc_resp_new_task_t resp = resp_future.get();

    return resp.err;
  }
}

std::optional<const Task*> TaskManager::FindTaskByName(
    const std::string& task_name) {
  auto iter = m_name_to_task_map_.find(task_name);
  if (iter == m_name_to_task_map_.end()) return std::nullopt;

  return iter->second.get();
}

std::string TaskManager::CgroupStrByPID(pid_t pid) {
  return fmt::format("SlurmX_proc_{}", pid);
}

void TaskManager::ev_sigchld_cb_(evutil_socket_t sig, short events,
                                 void* user_data) {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  sigchld_info_t sigchld_info;

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      if (WIFEXITED(status)) {
        // Exited with status WEXITSTATUS(status)
        sigchld_info = {pid, false, WEXITSTATUS(status)};
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        sigchld_info = {pid, true, WTERMSIG(status)};
      }
      /* Todo(More status tracing):
       else if (WIFSTOPPED(status)) {
        printf("stopped by signal %d\n", WSTOPSIG(status));
      } else if (WIFCONTINUED(status)) {
        printf("continued\n");
      } */

      std::string task_name;
      auto iter = this_->m_pid_to_name_map_.find(pid);
      if (iter == this_->m_pid_to_name_map_.end()) {
        SLURMX_ERROR("Failed to find task name for pid {}.", pid);
      } else {
        task_name = iter->second;
      }

      SLURMX_DEBUG("Received SIGCHLD. Destroying Task \"{}\".", task_name);

      this_->m_pid_to_name_map_.erase(pid);

      Task* task = this_->m_name_to_task_map_.find(task_name)->second.get();

      task->init_info.finish_callback(sigchld_info.is_terminated_by_signal,
                                      sigchld_info.value);

      bufferevent_free(task->ev_buf_event);

      this_->m_name_to_task_map_.erase(task_name);

      this_->m_cg_mgr_.destroy(CgroupStrByPID(pid));

      if (this_->m_is_ending_now_ && this_->m_pid_to_name_map_.empty()) {
        SLURMX_TRACE("ev_sigchld_cb_ has reaped all child. Stop event loop.");
        struct timeval delay = {0, 0};
        event_base_loopexit(this_->m_ev_base_, &delay);
      }
    } else if (pid == 0)  // There's no child that needs reaping.
      break;
    else if (pid < 0) {
      if (errno != ECHILD)
        SLURMX_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void TaskManager::ev_grpc_event_cb_(int efd, short events, void* user_data) {
  uint64_t u;
  ssize_t s;
  s = read(efd, &u, sizeof(uint64_t));
  if (s != sizeof(uint64_t)) {
    if (errno != EAGAIN) {
      SLURMX_ERROR("Failed to read grpc_fd: errno {}, {}", errno,
                   strerror(errno));
    }
    return;
  }

  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  grpc_req_new_task_t req;
  this_->m_gprc_new_task_queue_.try_dequeue(req);
  TaskInitInfo& task_init_info = req.task_init_info;

  SLURMX_DEBUG("Receive one grpc req of new task: {}", task_init_info.name);

  if (this_->m_name_to_task_map_.count(task_init_info.name) > 0) {
    req.resp_promise.set_value({SlurmxErr::kExistingTask});
    return;
  };

  constexpr u_char E_PIPE_OK = 0;
  constexpr u_char E_PIPE_SUICIDE = 1;
  AnonymousPipe anon_pipe;

  pid_t child_pid = fork();
  if (child_pid == 0) {  // Child proc
    anon_pipe.CloseParentEnd();

    SLURMX_DEBUG("Subprocess start running....");

    // We use u_char here, since the size of u_char is standard-defined.
    u_char val;

    // Blocking read to wait the parent move the child into designated cgroup.
    if (!anon_pipe.ReadIntegerFromParent<u_char>(&val))
      SLURMX_ERROR("Failed to write the expected 1 byte to AnonymousPipe.");

    // Use abort to avoid the wrong call to destructor of CgroupManager, which
    // deletes all cgroup in system.
    if (val == E_PIPE_SUICIDE) abort();

    // Set pgid to the root process of task.
    setpgid(0, 0);

    // Prepare the command line arguments.
    std::vector<const char*> argv;
    argv.push_back(task_init_info.executive_path.c_str());
    for (auto&& arg : task_init_info.arguments) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    SLURMX_TRACE("execv: {} {}", task_init_info.executive_path,
                 boost::algorithm::join(task_init_info.arguments, ", "));

    dup2(anon_pipe.GetChildEndFd(), 1);  // stdout -> pipe
    dup2(anon_pipe.GetChildEndFd(), 2);  // stderr -> pipe

    // Release the file descriptor before calling exec()
    anon_pipe.CloseChildEnd();

    execv(task_init_info.executive_path.c_str(),
          const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned.
    SLURMX_ERROR("execv failed: {}", strerror(errno));
    abort();
  } else {  // Parent proc
    SLURMX_TRACE("Child proc: pid {}", child_pid);

    u_char pipe_uchar_val;

    anon_pipe.CloseChildEnd();

    auto new_task = std::make_unique<Task>();
    new_task->root_pid = child_pid;
    new_task->cg_path = CgroupStrByPID(child_pid);
    new_task->init_info = std::move(task_init_info);

    // Avoid corruption during std::move
    std::string task_name_copy = new_task->init_info.name;

    // Create cgroup for the new subprocess
    if (!this_->m_cg_mgr_.create_or_open(CgroupStrByPID(child_pid),
                                         Cgroup::ALL_CONTROLLER_FLAG,
                                         Cgroup::NO_CONTROLLER_FLAG, false)) {
      SLURMX_ERROR(
          "Destroy child task process of \"{}\" due to failure of cgroup "
          "creation.",
          new_task->init_info.name);

      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        SLURMX_ERROR("Failed to send E_PIPE_SUICIDE to child.");
    }

    // Add event for stdout/stderr of the new subprocess
    new_task->ev_buf_event = bufferevent_socket_new(
        this_->m_ev_base_, anon_pipe.GetParentEndFd(), BEV_OPT_CLOSE_ON_FREE);
    if (!new_task->ev_buf_event) {
      SLURMX_ERROR(
          "Error constructing bufferevent for the subprocess of task \"{}\"!",
          task_init_info.name);
      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        SLURMX_ERROR("Failed to send E_PIPE_SUICIDE to child.");
    }
    bufferevent_setcb(new_task->ev_buf_event, ev_subprocess_read_cb_, nullptr,
                      nullptr, (void*)new_task.get());
    bufferevent_enable(new_task->ev_buf_event, EV_READ);
    bufferevent_disable(new_task->ev_buf_event, EV_WRITE);

    // Migrate the new subprocess to newly created cgroup
    if (!this_->m_cg_mgr_.migrate_proc_to_cgroup(new_task->root_pid,
                                                 new_task->cg_path)) {
      SLURMX_ERROR(
          "Destroy child task process of \"{}\" due to failure of cgroup "
          "migration.",
          new_task->init_info.name);

      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        SLURMX_ERROR("Failed to send E_PIPE_SUICIDE to child.");

      this_->m_cg_mgr_.destroy(new_task->cg_path);
      return;
    }

    SLURMX_TRACE("New task {} is ready. Asking subprocess to execv...",
                 new_task->init_info.name);
    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    pipe_uchar_val = E_PIPE_OK;
    if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
      SLURMX_ERROR("Failed to send E_PIPE_OK to child.");

    // Insert the task information into pid->name->Task mapping.
    this_->m_pid_to_name_map_.emplace(new_task->root_pid,
                                      new_task->init_info.name);
    this_->m_name_to_task_map_.emplace(std::move(task_name_copy),
                                       std::move(new_task));

    // Inform the async caller of success.
    req.resp_promise.set_value({SlurmxErr::kOk});
  }
}

void TaskManager::ev_subprocess_read_cb_(struct bufferevent* bev,
                                         void* task_ptr_) {
  auto task_ptr = reinterpret_cast<Task*>(task_ptr_);

  size_t buf_len = evbuffer_get_length(bev->input);

  std::string str;
  str.resize(buf_len);
  int n_copy = evbuffer_remove(bev->input, str.data(), buf_len);

  SLURMX_TRACE("Read {:>4} bytes from subprocess (pid: {}): {}", n_copy,
               task_ptr->root_pid, str);

  task_ptr->init_info.output_callback(std::move(str));
}

void TaskManager::ev_sigint_cb_(int sig, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  if (!this_->m_is_ending_now_) {
    SLURMX_INFO("Caught SIGINT. Send SIGINT to all running tasks...");

    this_->m_is_ending_now_ = true;

    if (this_->m_sigint_cb_) this_->m_sigint_cb_();

    if (this_->m_pid_to_name_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      struct timeval delay = {0, 0};
      event_base_loopexit(this_->m_ev_base_, &delay);
    } else {
      // Todo: Add timer which sends SIGTERM for those tasks who
      //  will not quit when receiving SIGINT.

      // Send SIGINT to all tasks and the event loop will stop
      // when the ev_sigchld_cb_ of the last task is called.
      for (auto&& elem : this_->m_pid_to_name_map_) {
        this_->Kill(elem.second, SIGINT);
      }
    }
  } else {
    SLURMX_INFO("SIGINT has been triggered already. Ignoring it.");
  }
}

void TaskManager::Wait() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
}

SlurmxErr TaskManager::Kill(const std::string& task_name, int signum) {
  auto task_option = FindTaskByName(task_name);
  if (task_option.has_value()) {
    const Task* task = task_option.value();

    // Send the signal to the whole process group.
    int err = kill(-task->root_pid, signum);

    if (err == 0)
      return SlurmxErr::kOk;
    else if (err == EINVAL)
      return SlurmxErr::kInvalidParam;
    else
      return SlurmxErr::kGenericFailure;
  }

  return SlurmxErr::kNonExistent;
}

void TaskManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = cb;
}
