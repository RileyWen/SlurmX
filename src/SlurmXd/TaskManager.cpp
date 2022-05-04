#include "TaskManager.h"

#include <absl/strings/str_split.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>

#include <utility>

#include "ResourceAllocators.h"
#include "protos/XdSubprocess.pb.h"
#include "slurmx/FileLogger.h"

namespace Xd {

TaskManager::TaskManager()
    : m_cg_mgr_(util::CgroupManager::Instance()),
      m_ev_sigchld_(nullptr),
      m_ev_base_(nullptr),
      m_ev_grpc_interactive_task_(nullptr),
      m_ev_exit_event_(nullptr),
      m_ev_task_status_change_(nullptr),
      m_is_ending_now_(false) {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_ev_base_ = event_base_new();
  if (!m_ev_base_) {
    SLURMX_ERROR("Could not initialize libevent!");
    std::terminate();
  }
  {  // SIGCHLD
    m_ev_sigchld_ = evsignal_new(m_ev_base_, SIGCHLD, EvSigchldCb_, this);
    if (!m_ev_sigchld_) {
      SLURMX_ERROR("Failed to create the SIGCHLD event!");
      std::terminate();
    }

    if (event_add(m_ev_sigchld_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the SIGCHLD event to base!");
      std::terminate();
    }
  }
  {  // SIGINT
    m_ev_sigint_ = evsignal_new(m_ev_base_, SIGINT, EvSigintCb_, this);
    if (!m_ev_sigint_) {
      SLURMX_ERROR("Failed to create the SIGCHLD event!");
      std::terminate();
    }

    if (event_add(m_ev_sigint_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the SIGINT event to base!");
      std::terminate();
    }
  }
  {  // gRPC: SpawnInteractiveTask
    m_ev_grpc_interactive_task_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcSpawnInteractiveTaskCb_, this);
    if (!m_ev_grpc_interactive_task_) {
      SLURMX_ERROR("Failed to create the grpc event!");
      std::terminate();
    }

    if (event_add(m_ev_grpc_interactive_task_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the grpc event to base!");
      std::terminate();
    }
  }
  {  // Exit Event
    if ((m_ev_exit_fd_ = eventfd(0, EFD_CLOEXEC)) < 0) {
      SLURMX_ERROR("Failed to init the eventfd!");
      std::terminate();
    }

    m_ev_exit_event_ = event_new(m_ev_base_, m_ev_exit_fd_,
                                 EV_PERSIST | EV_READ, EvExitEventCb_, this);
    if (!m_ev_exit_event_) {
      SLURMX_ERROR("Failed to create the exit event!");
      std::terminate();
    }

    if (event_add(m_ev_exit_event_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the exit event to base!");
      std::terminate();
    }
  }
  {  // Grpc Execute Task Event
    m_ev_grpc_execute_task_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                        EvGrpcExecuteTaskCb_, this);
    if (!m_ev_grpc_execute_task_) {
      SLURMX_ERROR("Failed to create the grpc_execute_task event!");
      std::terminate();
    }
    if (event_add(m_ev_grpc_execute_task_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the m_ev_grpc_execute_task_ to base!");
      std::terminate();
    }
  }
  {  // Task Status Change Event
    m_ev_task_status_change_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                         EvTaskStatusChangeCb_, this);
    if (!m_ev_task_status_change_) {
      SLURMX_ERROR("Failed to create the task_status_change event!");
      std::terminate();
    }
    if (event_add(m_ev_task_status_change_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the m_ev_task_status_change_event_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_task_terminate_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                     EvTerminateTaskCb_, this);
    if (!m_ev_task_terminate_) {
      SLURMX_ERROR("Failed to create the task_terminate event!");
      std::terminate();
    }
    if (event_add(m_ev_task_terminate_, nullptr) < 0) {
      SLURMX_ERROR("Could not add the m_ev_task_terminate_ to base!");
      std::terminate();
    }
  }

  m_ev_loop_thread_ =
      std::thread([this]() { event_base_dispatch(m_ev_base_); });
}

TaskManager::~TaskManager() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();

  if (m_ev_sigchld_) event_free(m_ev_sigchld_);
  if (m_ev_sigint_) event_free(m_ev_sigint_);

  if (m_ev_grpc_interactive_task_) event_free(m_ev_grpc_interactive_task_);

  if (m_ev_exit_event_) event_free(m_ev_exit_event_);
  close(m_ev_exit_fd_);

  if (m_ev_grpc_execute_task_) event_free(m_ev_grpc_execute_task_);

  if (m_ev_task_status_change_) event_free(m_ev_task_status_change_);

  if (m_ev_base_) event_base_free(m_ev_base_);
}

const TaskInstance* TaskManager::FindInstanceByTaskId_(uint32_t task_id) {
  auto iter = m_task_map_.find(task_id);
  if (iter == m_task_map_.end()) return nullptr;
  return iter->second.get();
}

std::string TaskManager::CgroupStrByTaskId_(uint32_t task_id) {
  return fmt::format("SlurmX_Task_{}", task_id);
}

void TaskManager::EvSigchldCb_(evutil_socket_t sig, short events,
                               void* user_data) {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  SigchldInfo sigchld_info{};

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

      uint32_t task_id;
      TaskInstance* instance;
      ProcessInstance* proc;

      auto task_iter = this_->m_pid_task_map_.find(pid);
      auto proc_iter = this_->m_pid_proc_map_.find(pid);
      if (task_iter == this_->m_pid_task_map_.end() ||
          proc_iter == this_->m_pid_proc_map_.end())
        SLURMX_ERROR("Failed to find task id for pid {}.", pid);
      else {
        instance = task_iter->second;
        proc = proc_iter->second;
        task_id = instance->task.task_id();

        proc->Finish(sigchld_info.is_terminated_by_signal, sigchld_info.value);

        // Free the ProcessInstance. ITask struct is not freed here because
        // the ITask for an Interactive task can have no ProcessInstance.
        auto pr_it = instance->processes.find(pid);
        if (pr_it == instance->processes.end()) {
          SLURMX_ERROR("Failed to find pid {} in task #{}'s ProcessInstances",
                       task_id, pid);
        } else {
          instance->processes.erase(pr_it);

          if (sigchld_info.is_terminated_by_signal) {
            instance->already_failed = true;
            this_->TerminateTaskAsync(task_id);
          }

          // Remove indexes from pid to ProcessInstance*
          this_->m_pid_proc_map_.erase(proc_iter);

          if (instance->processes.empty()) {
            // Remove indexes from pid to TaskInstance*
            this_->m_pid_task_map_.erase(task_iter);

            // See the comment of EvActivateTaskStatusChange_.
            if (instance->task.type() == SlurmxGrpc::Batch) {
              // For a Batch task, the end of the process means it is done.
              if (instance->already_failed)
                this_->EvActivateTaskStatusChange_(
                    task_id, SlurmxGrpc::TaskStatus::Failed, std::nullopt);
              else
                this_->EvActivateTaskStatusChange_(
                    task_id, SlurmxGrpc::TaskStatus::Finished, std::nullopt);
            } else {
              // For a COMPLETING Interactive task with a process running, the
              // end of this process means that this task is done.
              this_->EvActivateTaskStatusChange_(
                  task_id, SlurmxGrpc::TaskStatus::Finished, std::nullopt);
            }
          }
        }
      }

      // Todo: Add additional timer to check periodically whether all children
      //  have exited.
      if (this_->m_is_ending_now_ && this_->m_task_map_.empty()) {
        SLURMX_TRACE("EvSigchldCb_ has reaped all child. Stop event loop.");
        this_->Shutdown();
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

void TaskManager::EvSubprocessReadCb_(struct bufferevent* bev, void* process) {
  auto* proc = reinterpret_cast<ProcessInstance*>(process);

  size_t buf_len = evbuffer_get_length(bev->input);

  std::string str;
  str.resize(buf_len);
  int n_copy = evbuffer_remove(bev->input, str.data(), buf_len);

  SLURMX_TRACE("Read {:>4} bytes from subprocess (pid: {}): {}", n_copy,
               proc->GetPid(), str);

  proc->Output(std::move(str));
}

void TaskManager::EvSigintCb_(int sig, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  if (!this_->m_is_ending_now_) {
    SLURMX_INFO("Caught SIGINT. Send SIGINT to all running tasks...");

    this_->m_is_ending_now_ = true;

    if (this_->m_sigint_cb_) this_->m_sigint_cb_();

    if (this_->m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      this_->Shutdown();
    } else {
      // Send SIGINT to all tasks and the event loop will stop
      // when the ev_sigchld_cb_ of the last task is called.
      for (auto&& [task_id, task_instance] : this_->m_task_map_) {
        for (auto&& [pid, pr_instance] : task_instance->processes)
          KillProcessInstance_(pr_instance.get(), SIGINT);
      }
    }
  } else {
    SLURMX_INFO("SIGINT has been triggered already. Ignoring it.");
  }
}

void TaskManager::EvExitEventCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  SLURMX_TRACE("Exit event triggered. Stop event loop.");

  uint64_t u;
  ssize_t s;
  s = read(efd, &u, sizeof(uint64_t));
  if (s != sizeof(uint64_t)) {
    if (errno != EAGAIN) {
      SLURMX_ERROR("Failed to read exit_fd: errno {}, {}", errno,
                   strerror(errno));
    }
    return;
  }

  struct timeval delay = {0, 0};
  event_base_loopexit(this_->m_ev_base_, &delay);
}

void TaskManager::Shutdown() {
  SLURMX_TRACE("Triggering exit event...");
  m_is_ending_now_ = true;
  eventfd_t u = 1;
  ssize_t s = eventfd_write(m_ev_exit_fd_, u);
  if (s < 0) {
    SLURMX_ERROR("Failed to write to grpc event fd: {}", strerror(errno));
  }
}

void TaskManager::Wait() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
}

SlurmxErr TaskManager::KillProcessInstance_(const ProcessInstance* proc,
                                            int signum) {
  // Todo: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  if (proc) {
    // Send the signal to the whole process group.
    int err = kill(-proc->GetPid(), signum);

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
  m_sigint_cb_ = std::move(cb);
}

SlurmxErr TaskManager::SpawnProcessInInstance_(
    TaskInstance* instance, std::unique_ptr<ProcessInstance> process) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using SlurmxGrpc::Xd::CanStartMessage;

  int socket_pair[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, socket_pair) != 0) {
    SLURMX_ERROR("Failed to create socket pair: {}", strerror(errno));
    return SlurmxErr::kSystemErr;
  }

  // save the current uid/gid
  savedPrivilege saved_priv;
  saved_priv.uid = getuid();
  saved_priv.gid = getgid();
  saved_priv.cwd = get_current_dir_name();

  // int rc = setegid(instance->pwd_entry.Gid());
  // if (rc == -1) {
  //   SLURMX_ERROR("error: setegid. {}\n", strerror(errno));
  //   return SlurmxErr::kSystemErr;
  // }
//  __gid_t gid_a[1]={instance->pwd_entry.Gid()};
//  setgroups(1, gid_a);
  // rc = seteuid(instance->pwd_entry.Uid());
  // if (rc == -1) {
  //   SLURMX_ERROR("error: seteuid. {}\n", strerror(errno));
  //   return SlurmxErr::kSystemErr;
  // }
  // const std::string& cwd = instance->task.cwd();
  // rc = chdir(cwd.c_str());
  // if (rc == -1) {
  //   SLURMX_ERROR("error: chdir to {}. {}\n", cwd.c_str(), strerror(errno));
  //   return SlurmxErr::kSystemErr;
  // }

  pid_t child_pid = fork();
  if (child_pid > 0) {  // Parent proc
    close(socket_pair[1]);
    int fd = socket_pair[0];
    bool ok;
    SlurmxErr err;

    // setegid(saved_priv.gid);
    // seteuid(saved_priv.uid);
//    setgroups(0, nullptr);
    chdir(saved_priv.cwd.c_str());

    FileOutputStream ostream(fd);
    CanStartMessage msg;

    SLURMX_DEBUG("Subprocess was created for task #{} pid: {}",
                 instance->task.task_id(), child_pid);

    process->SetPid(child_pid);

    // Add event for stdout/stderr of the new subprocess
    struct bufferevent* ev_buf_event;
    ev_buf_event =
        bufferevent_socket_new(m_ev_base_, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!ev_buf_event) {
      SLURMX_ERROR(
          "Error constructing bufferevent for the subprocess of task #!",
          instance->task.task_id());
      err = SlurmxErr::kLibEventError;
      goto AskChildToSuicide;
    }
    bufferevent_setcb(ev_buf_event, EvSubprocessReadCb_, nullptr, nullptr,
                      (void*)process.get());
    bufferevent_enable(ev_buf_event, EV_READ);
    bufferevent_disable(ev_buf_event, EV_WRITE);

    process->SetEvBufEvent(ev_buf_event);

    // Migrate the new subprocess to newly created cgroup
    if (!m_cg_mgr_.MigrateProcTo(process->GetPid(), instance->cg_path)) {
      SLURMX_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          instance->task.task_id());

      m_cg_mgr_.Release(instance->cg_path);
      err = SlurmxErr::kCgroupError;
      goto AskChildToSuicide;
    }

    SLURMX_TRACE("New task #{} is ready. Asking subprocess to execv...",
                 instance->task.task_id());

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      SLURMX_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                   child_pid, instance->task.task_id());
      return SlurmxErr::kProtobufError;
    }

    // Add indexes from pid to TaskInstance*, ProcessInstance*
    m_pid_task_map_.emplace(child_pid, instance);
    m_pid_proc_map_.emplace(child_pid, process.get());

    // Move the ownership of ProcessInstance into the TaskInstance.
    instance->processes.emplace(child_pid, std::move(process));

    return SlurmxErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      SLURMX_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                   child_pid, instance->task.task_id());
      return SlurmxErr::kProtobufError;
    }
    return err;
  } else {  // Child proc
    // SLURMX_TRACE("Set reuid to {}, regid to {}", instance->pwd_entry.Uid(),
    //              instance->pwd_entry.Gid());
    // setreuid(instance->pwd_entry.Uid(), instance->pwd_entry.Gid());
    // setregid(instance->pwd_entry.Uid(), instance->pwd_entry.Gid());

    close(socket_pair[0]);
    int fd = socket_pair[1];

    FileInputStream istream(fd);
    CanStartMessage msg;

    ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
    if (!msg.ok()) std::abort();

    // Set pgid to the pid of task root process.
    setpgid(0, 0);

    std::vector<std::string> env_vec =
        absl::StrSplit(instance->task.env(), "||");
    for (auto str : env_vec) {
      if (putenv(str.data())) {
        SLURMX_ERROR("set environ {} failed!", str);
      }
    }

    // Prepare the command line arguments.
    std::vector<const char*> argv;
    argv.push_back(process->GetExecPath().c_str());
    for (auto&& arg : process->GetArgList()) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    SLURMX_TRACE("execv being called subprocess: {} {}", process->GetExecPath(),
                 boost::algorithm::join(process->GetArgList(), " "));

    dup2(fd, 1);  // stdout -> pipe
    dup2(fd, 2);  // stderr -> pipe

    close(fd);

    execv(process->GetExecPath().c_str(),
          const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // CtlXd use SIGABRT to inform the client of this failure.
    fmt::print(stderr,
               "[SlurmCtlXd Subprocess Error] Failed to execv. Error: {}\n",
               strerror(errno));
    // Todo: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }
}

SlurmxErr TaskManager::ExecuteTaskAsync(SlurmxGrpc::TaskToXd task) {
  auto instance = std::make_unique<TaskInstance>();

  // Simply wrap the Task structure within a TaskInstance structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  instance->task = std::move(task);

  m_grpc_execute_task_queue_.enqueue(std::move(instance));
  event_active(m_ev_grpc_execute_task_, 0, 0);

  return SlurmxErr::kOk;
}

void TaskManager::EvGrpcExecuteTaskCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);
  std::unique_ptr<TaskInstance> popped_instance;

  while (this_->m_grpc_execute_task_queue_.try_dequeue(popped_instance)) {
    // Once ExecuteTask RPC is processed, the TaskInstance goes into
    // m_task_map_.
    auto [iter, ok] = this_->m_task_map_.emplace(
        popped_instance->task.task_id(), std::move(popped_instance));

    TaskInstance* instance = iter->second.get();
    instance->pwd_entry.Init(instance->task.uid());
    if (!instance->pwd_entry.Valid()) {
      SLURMX_DEBUG("Failed to look up password entry for uid {} of task #{}",
                   instance->task.uid(), instance->task.task_id());
      this_->EvActivateTaskStatusChange_(
          instance->task.task_id(), SlurmxGrpc::TaskStatus::Failed,
          fmt::format("Failed to look up password entry for uid {} of task #{}",
                      instance->task.uid(), instance->task.task_id()));
      return;
    }

    instance->cg_path = CgroupStrByTaskId_(instance->task.task_id());
    util::Cgroup* cgroup;
    cgroup = this_->m_cg_mgr_.CreateOrOpen(instance->cg_path,
                                           util::ALL_CONTROLLER_FLAG,
                                           util::NO_CONTROLLER_FLAG, false);
    // Create cgroup for the new subprocess
    if (!cgroup) {
      SLURMX_ERROR("Failed to create cgroup for task #{}",
                   instance->task.task_id());
      this_->EvActivateTaskStatusChange_(
          instance->task.task_id(), SlurmxGrpc::TaskStatus::Failed,
          fmt::format("Cannot create cgroup for the instance of task #{}",
                      instance->task.task_id()));
      return;
    }

    if (!AllocatableResourceAllocator::Allocate(
            instance->task.resources().allocatable_resource(), cgroup)) {
      SLURMX_ERROR(
          "Failed to allocate allocatable resource in cgroup for task #{}",
          instance->task.task_id());
      this_->EvActivateTaskStatusChange_(
          instance->task.task_id(), SlurmxGrpc::TaskStatus::Failed,
          fmt::format("Cannot allocate resources for the instance of task #{}",
                      instance->task.task_id()));
      return;
    }

    // If this is a batch task, run it now.
    if (instance->task.type() == SlurmxGrpc::Batch) {
      instance->batch_meta.parsed_sh_script_path = fmt::format(
          "/tmp/slurmxd/scripts/slurmx-{}.sh", instance->task.task_id());
      auto& sh_path = instance->batch_meta.parsed_sh_script_path;

      FILE* fptr = fopen(sh_path.c_str(), "w");
      if (fptr == nullptr) {
        SLURMX_ERROR("Cannot write shell script for batch task #{}",
                     instance->task.task_id());
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), SlurmxGrpc::TaskStatus::Failed,
            fmt::format("Cannot write shell script for batch task #{}",
                        instance->task.task_id()));
        return;
      }
      fputs(instance->task.batch_meta().sh_script().c_str(), fptr);
      fclose(fptr);

      chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

      SlurmxErr err = SlurmxErr::kOk;
      for (int i = 0; i < instance->task.task_per_node(); ++i) {
        auto process = std::make_unique<ProcessInstance>(
            sh_path, std::list<std::string>());

        process->batch_meta.parsed_output_file_pattern =
            fmt::format("{}/{}-{}-{}.out", instance->task.cwd(),
                        instance->task.batch_meta().output_file_pattern(),
                        g_ctlxd_client->GetNodeId().node_index, i);
        boost::replace_all(process->batch_meta.parsed_output_file_pattern, "%A",
                           std::to_string(instance->task.task_id()));

        auto* file_logger = new slurmx::FileLogger(
            fmt::format("{}-{}", instance->task.task_id(), i),
            process->batch_meta.parsed_output_file_pattern);

        auto clean_cb = [](void* data) {
          delete reinterpret_cast<slurmx::FileLogger*>(data);
        };

        auto output_cb = [](std::string&& buf, void* data) {
          auto* file_logger = reinterpret_cast<slurmx::FileLogger*>(data);
          file_logger->Output(buf);
        };

        process->SetUserDataAndCleanCb(file_logger, std::move(clean_cb));
        process->SetOutputCb(std::move(output_cb));

        err = this_->SpawnProcessInInstance_(instance, std::move(process));
        if (err != SlurmxErr::kOk) break;
      }

      if (err != SlurmxErr::kOk) {
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), SlurmxGrpc::TaskStatus::Failed,
            fmt::format(
                "Cannot spawn a new process inside the instance of task #{}",
                instance->task.task_id()));
      }
    }

    // Add a timer to limit the execution time of a task.
    this_->EvAddTerminationTimer_(instance,
                                  instance->task.time_limit().seconds());
  }
}

void TaskManager::EvTaskStatusChangeCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  TaskStatusChange status_change;
  while (this_->m_task_status_change_queue_.try_dequeue(status_change)) {
    if (status_change.new_status == SlurmxGrpc::TaskStatus::Finished ||
        status_change.new_status == SlurmxGrpc::TaskStatus::Failed) {
      SLURMX_DEBUG(R"(Destroying Task structure for "{}".)",
                   status_change.task_id);

      auto iter = this_->m_task_map_.find(status_change.task_id);
      SLURMX_ASSERT_MSG(iter != this_->m_task_map_.end(),
                        "Task should be found here.");

      TaskInstance* task_instance = iter->second.get();

      if (task_instance->termination_timer) {
        event_del(task_instance->termination_timer);
        event_free(task_instance->termination_timer);
      }

      // Destroy the related cgroup.
      this_->m_cg_mgr_.Release(task_instance->cg_path);
      SLURMX_DEBUG("Received SIGCHLD. Destroying Cgroup for task #{}",
                   status_change.task_id);

      // Free the TaskInstance structure
      this_->m_task_map_.erase(status_change.task_id);
    }

    g_ctlxd_client->TaskStatusChangeAsync(std::move(status_change));
  }
}

void TaskManager::EvActivateTaskStatusChange_(
    uint32_t task_id, SlurmxGrpc::TaskStatus new_status,
    std::optional<std::string> reason) {
  TaskStatusChange status_change{task_id, new_status};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  event_active(m_ev_task_status_change_, 0, 0);
}

SlurmxErr TaskManager::SpawnInteractiveTaskAsync(
    uint32_t task_id, std::string executive_path,
    std::list<std::string> arguments,
    std::function<void(std::string&&, void*)> output_cb,
    std::function<void(bool, int, void*)> finish_cb) {
  EvQueueGrpcInteractiveTask elem{
      .task_id = task_id,
      .executive_path = std::move(executive_path),
      .arguments = std::move(arguments),
      .output_cb = std::move(output_cb),
      .finish_cb = std::move(finish_cb),
  };
  std::future<SlurmxErr> err_future = elem.err_promise.get_future();

  m_grpc_interactive_task_queue_.enqueue(std::move(elem));
  event_active(m_ev_grpc_interactive_task_, 0, 0);

  return err_future.get();
}

void TaskManager::EvGrpcSpawnInteractiveTaskCb_(int efd, short events,
                                                void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueGrpcInteractiveTask elem;
  while (this_->m_grpc_interactive_task_queue_.try_dequeue(elem)) {
    SLURMX_TRACE("Receive one GrpcSpawnInteractiveTask for task #{}",
                 elem.task_id);

    auto task_iter = this_->m_task_map_.find(elem.task_id);
    if (task_iter == this_->m_task_map_.end()) {
      SLURMX_ERROR("Cannot find task #{}", elem.task_id);
      elem.err_promise.set_value(SlurmxErr::kNonExistent);
      return;
    }

    if (task_iter->second->task.type() != SlurmxGrpc::Interactive) {
      SLURMX_ERROR("Try spawning a new process in non-interactive task #{}!",
                   elem.task_id);
      elem.err_promise.set_value(SlurmxErr::kInvalidParam);
      return;
    }

    auto process = std::make_unique<ProcessInstance>(
        std::move(elem.executive_path), std::move(elem.arguments));

    process->SetOutputCb(std::move(elem.output_cb));
    process->SetFinishCb(std::move(elem.finish_cb));

    SlurmxErr err;
    err = this_->SpawnProcessInInstance_(task_iter->second.get(),
                                         std::move(process));
    elem.err_promise.set_value(err);

    if (err != SlurmxErr::kOk)
      this_->EvActivateTaskStatusChange_(elem.task_id, SlurmxGrpc::Failed,
                                         std::string(SlurmxErrStr(err)));
  }
}

void TaskManager::EvOnTimerCb_(int, short, void* arg_) {
  auto* arg = reinterpret_cast<EvTimerCbArg*>(arg_);
  TaskManager* this_ = arg->task_manager;

  SLURMX_TRACE("Task #{} exceeded its time limit. Terminating it...",
               arg->task_instance->task.task_id());

  EvQueueTaskTerminate ev_task_terminate{arg->task_instance->task.task_id()};
  this_->m_task_terminate_queue_.enqueue(ev_task_terminate);
  event_active(this_->m_ev_task_terminate_, 0, 0);

  event_del(arg->timer_ev);
  event_free(arg->timer_ev);
  arg->task_instance->termination_timer = nullptr;
}

void TaskManager::EvTerminateTaskCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueTaskTerminate elem;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  while (this_->m_task_terminate_queue_.try_dequeue(elem)) {
    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter == this_->m_task_map_.end()) {
      SLURMX_ERROR("Trying terminating unknown task #{}", elem.task_id);
      return;
    }

    const auto& task_instance = iter->second;

    int sig = SIGTERM;  // For BatchTask
    if (task_instance->task.type() == SlurmxGrpc::Interactive) sig = SIGHUP;

    if (!task_instance->processes.empty()) {
      // For an Interactive task with a process running or a Batch task, we just
      // send a kill signal here.
      for (auto&& [pid, pr_instance] : task_instance->processes)
        KillProcessInstance_(pr_instance.get(), sig);
    } else {
      // For an Interactive task with no process running, it ends immediately.
      this_->EvActivateTaskStatusChange_(elem.task_id, SlurmxGrpc::Finished,
                                         std::nullopt);
    }
  }
}

void TaskManager::TerminateTaskAsync(uint32_t task_id) {
  EvQueueTaskTerminate elem{task_id};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

}  // namespace Xd