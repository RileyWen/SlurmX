#include "TaskManager.h"

#include <evrpc.h>

TaskManager::TaskManager()
    : m_cg_mgr_(CgroupManager::getInstance()),
      m_ev_sigchld_(nullptr),
      m_ev_base_(nullptr),
      m_ev_grpc_event_(nullptr) {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_ev_base_ = event_base_new();
  if (!m_ev_base_) {
    spdlog::error("Could not initialize libevent!");
    abort();
  }

  m_ev_sigchld_ = evsignal_new(m_ev_base_, SIGCHLD, ev_sigchld_cb_, this);
  if (!m_ev_sigchld_) {
    spdlog::error("Failed to create the SIGCHLD event!");
    abort();
  }

  if (event_add(m_ev_sigchld_, nullptr) < 0) {
    spdlog::error("Could not add the SIGCHLD event to base!");
    abort();
  }

  m_ev_sigint_ = evsignal_new(m_ev_base_, SIGINT, ev_sigint_cb_, this);
  if (!m_ev_sigint_) {
    spdlog::error("Failed to create the SIGCHLD event!");
    abort();
  }

  if (event_add(m_ev_sigint_, nullptr) < 0) {
    spdlog::error("Could not add the SIGINT event to base!");
    abort();
  }

  if ((m_grpc_event_fd_ = eventfd(0, EFD_SEMAPHORE)) < 0) {
    spdlog::error("Failed to init the eventfd!");
    abort();
  }

  m_ev_grpc_event_ = event_new(m_ev_base_, m_grpc_event_fd_,
                               EV_PERSIST | EV_READ, ev_grpc_event_cb_, this);
  if (!m_ev_grpc_event_) {
    spdlog::error("Failed to create the grpc event!");
    abort();
  }

  if (event_add(m_ev_grpc_event_, nullptr) < 0) {
    spdlog::error("Could not add the grpc event to base!");
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

std::future<grpc_resp_t> TaskManager::AddTaskAsync(TaskInfo&& task_info) {
  eventfd_t u = 1;

  std::promise<grpc_resp_t> resp_prom;
  std::future<grpc_resp_t> resp_future = resp_prom.get_future();

  grpc_req_t req{
      std::move(task_info),
      std::move(resp_prom),
  };

  m_gprc_reqs_.enqueue(std::move(req));
  ssize_t s = eventfd_write(m_grpc_event_fd_, u);
  if (s < 0) {
    spdlog::error("Failed to write to grpc event fd: {}", strerror(errno));
  }
  return resp_future;
}

std::optional<TaskInfoCRef> TaskManager::FindTaskInfoByName(
    const std::string& task_name) {
  auto iter = m_name_to_task_map_.find(task_name);
  if (iter == m_name_to_task_map_.end()) return std::nullopt;

  return iter->second.info;
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
        sigchld_info = {pid,
                        sigchld_info_t::return_type_t::RET_VAL,
                        {WEXITSTATUS(status)}};
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        sigchld_info = {pid,
                        sigchld_info_t::return_type_t::SIGNAL,
                        {WTERMSIG(status)}};
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
        spdlog::error("Failed to find task name for pid {}.", pid);
      } else {
        task_name = iter->second;
      }

      this_->m_pid_to_name_map_.erase(pid);

      const Task& task = this_->m_name_to_task_map_.find(task_name)->second;
      bufferevent_free(task.ev_buf_event);

      this_->m_name_to_task_map_.erase(task_name);

      this_->m_cg_mgr_.destroy(CgroupStrByPID(pid));
      spdlog::debug("Received SIGCHLD. Destroying Task \"{}\".", task_name);

      // TODO: Add task termination notification to front-end.
    } else if (pid == 0)  // There's no child that needs reaping.
      break;
    else if (pid < 0) {
      if (errno != ECHILD)
        spdlog::debug("waitpid() error: {}, {}", errno, strerror(errno));
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
      spdlog::error("Failed to read grpc_fd: errno {}, {}", errno,
                    strerror(errno));
    }
    return;
  }

  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  grpc_req_t req;
  this_->m_gprc_reqs_.try_dequeue(req);
  TaskInfo& task_info = req.task_info;

  spdlog::debug("Receive one grpc req.");

  if (this_->m_name_to_task_map_.count(task_info.name) > 0) {
    req.resp_promise.set_value(
        {false, fmt::format("Task \"{}\" already exists.", task_info.name)});
    return;
  };

  constexpr u_char E_PIPE_OK = 0;
  constexpr u_char E_PIPE_SUICIDE = 1;
  AnonymousPipe anon_pipe;

  pid_t child_pid = fork();
  if (child_pid == 0) {  // Child proc
    anon_pipe.CloseParentEnd();

    spdlog::debug("Subprocess start running....");

    dup2(anon_pipe.GetChildEndFd(), 1);  // stdout -> pipe
    dup2(anon_pipe.GetChildEndFd(), 2);  // stderr -> pipe

    // We use u_char here, since the size of u_char is standard-defined.
    u_char val;

    // Blocking read to wait the parent move the child into designated cgroup.
    if (!anon_pipe.ReadIntegerFromParent<u_char>(&val))
      spdlog::error("Failed to write the expected 1 byte to AnonymousPipe.");

    // Use abort to avoid the wrong call to destructor of CgroupManager, which
    // deletes all cgroup in system.
    if (val == E_PIPE_SUICIDE) abort();

    // Release the file descriptor before calling exec()
    anon_pipe.CloseChildEnd();

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

    anon_pipe.CloseChildEnd();

    Task new_task;
    new_task.root_pid = child_pid;
    new_task.cg_path = CgroupStrByPID(child_pid);
    new_task.info = std::move(task_info);

    // Avoid corruption during std::move
    std::string task_name_copy = new_task.info.name;

    // Create cgroup for the new subprocess
    if (!this_->m_cg_mgr_.create_or_open(CgroupStrByPID(child_pid),
                                         ALL_CONTROLLER_FLAG,
                                         NO_CONTROLLER_FLAG, false)) {
      spdlog::error(
          "Destroy child task process of \"{}\" due to failure of cgroup "
          "creation.",
          new_task.info.name);

      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        spdlog::error("Failed to send E_PIPE_SUICIDE to child.");
    }

    // Add event for stdout/stderr of the new subprocess
    new_task.ev_buf_event = bufferevent_socket_new(
        this_->m_ev_base_, anon_pipe.GetParentEndFd(), BEV_OPT_CLOSE_ON_FREE);
    if (!new_task.ev_buf_event) {
      spdlog::error(
          "Error constructing bufferevent for the subprocess of task \"{}\"!",
          task_info.name);
      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        spdlog::error("Failed to send E_PIPE_SUICIDE to child.");
    }
    bufferevent_setcb(new_task.ev_buf_event, ev_subprocess_read_cb_, nullptr,
                      nullptr, (void*)static_cast<std::uintptr_t>(child_pid));
    bufferevent_enable(new_task.ev_buf_event, EV_READ);
    bufferevent_disable(new_task.ev_buf_event, EV_WRITE);

    // Migrate the new subprocess to newly created cgroup
    if (!this_->m_cg_mgr_.migrate_proc_to_cgroup(new_task.root_pid,
                                                 new_task.cg_path)) {
      spdlog::error(
          "Destroy child task process of \"{}\" due to failure of cgroup "
          "migration.",
          new_task.info.name);

      pipe_uchar_val = E_PIPE_SUICIDE;
      if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
        spdlog::error("Failed to send E_PIPE_SUICIDE to child.");

      this_->m_cg_mgr_.destroy(new_task.cg_path);
      return;
    }

    pipe_uchar_val = E_PIPE_OK;
    if (!anon_pipe.WriteIntegerToChild<u_char>(pipe_uchar_val))
      spdlog::error("Failed to send E_PIPE_OK to child.");

    this_->m_pid_to_name_map_.emplace(new_task.root_pid, new_task.info.name);
    this_->m_name_to_task_map_.emplace(std::move(task_name_copy),
                                       std::move(new_task));

    req.resp_promise.set_value({true, std::nullopt});
  }
}

void TaskManager::ev_subprocess_read_cb_(struct bufferevent* bev, void* pid_) {
  pid_t pid = reinterpret_cast<std::uintptr_t>(pid_);

  size_t buf_len = evbuffer_get_length(bev->input);

  char str_[buf_len + 1];
  int n_copy = evbuffer_remove(bev->input, str_, buf_len);
  str_[buf_len] = '\0';
  std::string_view str(str_);

  spdlog::info("Read {:>4} bytes from subprocess (pid: {}): {}", n_copy, pid,
               str);
}

void TaskManager::ev_sigint_cb_(int sig, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);
  struct timeval delay = {0, 0};

  spdlog::info("Caught an interrupt signal; exiting cleanly ...");

  event_base_loopexit(this_->m_ev_base_, &delay);
}

void TaskManager::Wait() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
}
