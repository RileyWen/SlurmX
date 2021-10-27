#include "TaskManager.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <utility>

#include "ResourceAllocators.h"
#include "protos/XdSubprocess.pb.h"

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
        task_id = instance->task->task_id;

        proc->Finish(sigchld_info.is_terminated_by_signal, sigchld_info.value);

        // Free the ProcessInstance. ITask struct is not freed here because
        // the ITask for an Interactive task can have no ProcessInstance.
        instance->process.reset(nullptr);

        // Remove indexes from pid to TaskInstance*, ProcessInstance*
        this_->m_pid_task_map_.erase(task_iter);
        this_->m_pid_proc_map_.erase(proc_iter);

        // See the comment of EvActivateTaskStatusChange_.
        if (instance->task->type == ITask::Type::Batch) {
          // For a Batch task, the end of the process means it is done.
          if (sigchld_info.is_terminated_by_signal)
            this_->EvActivateTaskStatusChange_(task_id, ITask::Status::Failed,
                                               std::nullopt);
          else
            this_->EvActivateTaskStatusChange_(task_id, ITask::Status::Finished,
                                               std::nullopt);
        } else {
          // For a COMPLETING Interactive task with a process running, the end
          // of this process means that this task is done.
          this_->EvActivateTaskStatusChange_(task_id, ITask::Status::Finished,
                                             std::nullopt);
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
      for (auto&& [task_id, instance] : this_->m_task_map_) {
        KillProcessInstance_(instance->process.get(), SIGINT);
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

  pid_t child_pid = fork();
  if (child_pid > 0) {  // Parent proc
    close(socket_pair[1]);
    int fd = socket_pair[0];
    bool ok;
    SlurmxErr err;

    FileOutputStream ostream(fd);
    CanStartMessage msg;

    SLURMX_DEBUG("Subprocess was created for task #{} pid: {}",
                 instance->task->task_id, child_pid);

    process->SetPid(child_pid);

    // Add event for stdout/stderr of the new subprocess
    struct bufferevent* ev_buf_event;
    ev_buf_event =
        bufferevent_socket_new(m_ev_base_, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!ev_buf_event) {
      SLURMX_ERROR(
          "Error constructing bufferevent for the subprocess of task #!",
          instance->task->task_id);
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
          instance->task->task_id);

      m_cg_mgr_.Release(instance->cg_path);
      err = SlurmxErr::kCgroupError;
      goto AskChildToSuicide;
    }

    SLURMX_TRACE("New task #{} is ready. Asking subprocess to execv...",
                 instance->task->task_id);

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      SLURMX_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                   child_pid, instance->task->task_id);
      return SlurmxErr::kProtobufError;
    }

    // Add indexes from pid to TaskInstance*, ProcessInstance*
    m_pid_task_map_.emplace(child_pid, instance);
    m_pid_proc_map_.emplace(child_pid, process.get());

    // Move the ownership of ProcessInstance into the TaskInstance.
    instance->process = std::move(process);

    return SlurmxErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      SLURMX_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                   child_pid, instance->task->task_id);
      return SlurmxErr::kProtobufError;
    }
    return err;
  } else {  // Child proc
    close(socket_pair[0]);
    int fd = socket_pair[1];

    FileInputStream istream(fd);
    CanStartMessage msg;

    ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
    if (!msg.ok()) std::abort();

    // Set pgid to the pid of task root process.
    setpgid(0, 0);

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

SlurmxErr TaskManager::ExecuteTaskAsync(std::unique_ptr<ITask> task) {
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

  if (!this_->m_grpc_execute_task_queue_.try_dequeue(popped_instance)) {
    std::string err_str{fmt::format(
        "GrpcExecuteTask was notified, but m_grpc_execute_task_queue_ is "
        "empty in task #{}",
        popped_instance->task->task_id)};
    SLURMX_ERROR(err_str);
    this_->EvActivateTaskStatusChange_(popped_instance->task->task_id,
                                       ITask::Status::Failed,
                                       std::move(err_str));
    return;
  }

  // Once ExecuteTask RPC is processed, the TaskInstance goes into m_task_map_.
  auto [iter, ok] = this_->m_task_map_.emplace(popped_instance->task->task_id,
                                               std::move(popped_instance));

  TaskInstance* instance = iter->second.get();
  instance->cg_path = CgroupStrByTaskId_(instance->task->task_id);
  util::Cgroup* cgroup;
  cgroup = this_->m_cg_mgr_.CreateOrOpen(instance->cg_path,
                                         util::ALL_CONTROLLER_FLAG,
                                         util::NO_CONTROLLER_FLAG, false);
  // Create cgroup for the new subprocess
  if (!cgroup) {
    SLURMX_ERROR("Failed to create cgroup for task #{}",
                 instance->task->task_id);
    this_->EvActivateTaskStatusChange_(
        instance->task->task_id, ITask::Status::Failed,
        fmt::format("Cannot create cgroup for the instance of task #{}",
                    instance->task->task_id));
    return;
  }

  if (!AllocatableResourceAllocator::Allocate(
          instance->task->resources.allocatable_resource, cgroup)) {
    SLURMX_ERROR(
        "Failed to allocate allocatable resource in cgroup for task #{}",
        instance->task->task_id);
    this_->EvActivateTaskStatusChange_(
        instance->task->task_id, ITask::Status::Failed,
        fmt::format("Cannot allocate resources for the instance of task #{}",
                    instance->task->task_id));
    return;
  }

  // If this is a batch task, run it now.
  if (instance->task->type == ITask::Type::Batch) {
    const auto* batch_task = dynamic_cast<BatchTask*>(instance->task.get());
    auto process = std::make_unique<ProcessInstance>(batch_task->executive_path,
                                                     batch_task->arguments);

    SlurmxErr err;
    err = this_->SpawnProcessInInstance_(instance, std::move(process));
    if (err != SlurmxErr::kOk) {
      this_->EvActivateTaskStatusChange_(
          instance->task->task_id, ITask::Status::Failed,
          fmt::format(
              "Cannot spawn a new process inside the instance of task #{}",
              instance->task->task_id));
    }
  }

  // Todo: Add timer for the time limit here!
}

void TaskManager::EvTaskStatusChangeCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  TaskStatusChange status_change;
  this_->m_task_status_change_queue_.try_dequeue(status_change);

  if (status_change.new_status == ITask::Status::Finished ||
      status_change.new_status == ITask::Status::Failed) {
    SLURMX_DEBUG(R"(Destroying Task structure for "{}".)",
                 status_change.task_id);

    auto iter = this_->m_task_map_.find(status_change.task_id);
    SLURMX_ASSERT(iter != this_->m_task_map_.end(),
                  "Task should be found here.");

    // Destroy the related cgroup.
    this_->m_cg_mgr_.Release(iter->second->cg_path);
    SLURMX_DEBUG("Received SIGCHLD. Destroying Cgroup for task #{}",
                 status_change.task_id);

    // Free the TaskInstance structure
    this_->m_task_map_.erase(status_change.task_id);
  }

  g_ctlxd_client->TaskStatusChangeAsync(std::move(status_change));
}

void TaskManager::EvActivateTaskStatusChange_(
    uint32_t task_id, ITask::Status new_status,
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
  this_->m_grpc_interactive_task_queue_.try_dequeue(elem);

  SLURMX_TRACE("Receive one GrpcSpawnInteractiveTask for task #{}",
               elem.task_id);

  auto task_iter = this_->m_task_map_.find(elem.task_id);
  if (task_iter == this_->m_task_map_.end()) {
    SLURMX_ERROR("Cannot find task #{}", elem.task_id);
    elem.err_promise.set_value(SlurmxErr::kNonExistent);
    return;
  }

  if (task_iter->second->task->type != ITask::Type::Interactive) {
    SLURMX_ERROR("Try spawning a new process in non-interactive task #{}!",
                 elem.task_id);
    elem.err_promise.set_value(SlurmxErr::kInvalidParam);
    return;
  }

  if (task_iter->second->task->status == ITask::Status::Completing) {
    // If someone has called TerminateTask, don't spawn a new process in this
    // task.
    elem.err_promise.set_value(SlurmxErr::kStop);
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
    this_->EvActivateTaskStatusChange_(elem.task_id, ITask::Status::Failed,
                                       std::string(SlurmxErrStr(err)));
}

void TaskManager::EvTerminateTaskCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueTaskTerminate elem;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  this_->m_task_terminate_queue_.try_dequeue(elem);

  auto iter = this_->m_task_map_.find(elem.task_id);
  if (iter == this_->m_task_map_.end()) {
    SLURMX_ERROR("Trying terminating unknown task #{}", elem.task_id);
    return;
  }

  const auto& task_instance = iter->second;
  task_instance->task->status = ITask::Status::Completing;

  int sig = SIGTERM;  // For BatchTask
  if (task_instance->task->type == ITask::Type::Interactive) sig = SIGHUP;

  if (task_instance->process) {
    // For an Interactive task with a process running or a Batch task, we just
    // send a kill signal here.
    KillProcessInstance_(task_instance->process.get(), sig);
  } else {
    // For an Interactive task with no process running, it ends immediately.
    this_->EvActivateTaskStatusChange_(elem.task_id, ITask::Status::Finished,
                                       std::nullopt);
  }
}

void TaskManager::TerminateTaskAsync(uint32_t task_id) {
  EvQueueTaskTerminate elem{task_id};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

}  // namespace Xd