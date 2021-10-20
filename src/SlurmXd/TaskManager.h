#pragma once

#include <absl/container/flat_hash_map.h>
#include <concurrentqueue/concurrentqueue.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <evrpc.h>
#include <grpc++/grpc++.h>
#include <spdlog/spdlog.h>
#include <sys/eventfd.h>
#include <sys/wait.h>

#include <any>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <csignal>
#include <forward_list>
#include <functional>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "CtlXdClient.h"
#include "XdPublicDefs.h"
#include "cgroup.linux.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "slurmx/PublicHeader.h"

namespace Xd {

struct ProcessInstance {
  /* ------------- Fields set by SpawnProcessInInstance_  ---------------- */
  pid_t pid;

  // The underlying event that handles the output of the task.
  struct bufferevent* ev_buf_event;

  /* ------- Fields set by the caller of SpawnProcessInInstance_  -------- */
  std::string executive_path;
  std::list<std::string> arguments;

  /***
   * The callback function called when a task writes to stdout or stderr.
   * @param[in] buf a slice of output buffer.
   */
  std::function<void(std::string&& buf)> output_cb;

  /***
   * The callback function called when a task is finished.
   * @param[in] bool true if the task is terminated by a signal, false
   * otherwise.
   * @param[in] int the number of signal if bool is true, the return value
   * otherwise.
   */
  std::function<void(bool, int)> finish_cb;
};

// Todo: Task may consists of multiple subtasks
struct TaskInstance {
  std::unique_ptr<ITask> task;

  // fields below are some kinds of Task runtime information

  // The cgroup name that restrains the TaskInstance.
  std::string cg_path;

  std::unique_ptr<ProcessInstance> process;
};

/**
 * The class that manages all tasks and handles interrupts.
 * SIGINT and SIGCHLD are processed in TaskManager.
 * Especially, outside caller can use SetSigintCallback() to
 * set the callback when SIGINT is triggered.
 */
class TaskManager {
 public:
  TaskManager();

  ~TaskManager();

  SlurmxErr ExecuteTaskAsync(std::unique_ptr<ITask> task);

  SlurmxErr SpawnInteractiveTaskAsync(
      uint32_t task_id, std::string executive_path,
      std::list<std::string> arguments,
      std::function<void(std::string&& buf)> output_cb,
      std::function<void(bool, int)> finish_cb);

  void TerminateTaskAsync(uint32_t task_id);

  // Wait internal libevent base loop to exit...
  void Wait();

  // Ask TaskManager to stop its event loop.
  void Shutdown();

  /***
   * Set the callback function will be called when SIGINT is triggered.
   * This function is not thread-safe.
   * @param cb the callback function.
   */
  void SetSigintCallback(std::function<void()> cb);

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct SigchldInfo {
    pid_t pid;
    bool is_terminated_by_signal;
    int value;
  };

  struct EvQueueGrpcInteractiveTask {
    std::promise<SlurmxErr> err_promise;
    uint32_t task_id;
    std::string executive_path;
    std::list<std::string> arguments;
    std::function<void(std::string&& buf)> output_cb;
    std::function<void(bool, int)> finish_cb;
  };

  struct EvQueueTaskTerminate {
    uint32_t task_id;
  };

  static std::string CgroupStrByTaskId_(uint32_t task_id);

  /**
   * EvActivateTaskStatusChange_ must NOT be called in this method and should be
   *  called in the caller method after checking the return value of this
   *  method.
   * @return kSystemErr if the socket pair between the parent process and child
   *  process cannot be created, and the caller should call strerror() to check
   *  the unix error code. kLibEventError if bufferevent_socket_new() fails.
   *  kCgroupError if CgroupManager cannot move the process to the cgroup bound
   *  to the TaskInstance. kProtobufError if the communication between the
   *  parent and the child process fails.
   */
  SlurmxErr SpawnProcessInInstance_(TaskInstance* instance,
                                    std::unique_ptr<ProcessInstance> process);

  const TaskInstance* FindInstanceByTaskId_(uint32_t task_id);

  /**
   * Inform SlurmCtlXd of the status change of a task.
   * This method is called when the status of a task is changed:
   * 1. A task is finished successfully. It means that this task returns
   *  normally with 0 or a non-zero code. (EvSigchldCb_)
   * 2. A task is killed by a signal. In this case, the task is considered
   *  failed. (EvSigchldCb_)
   * 3. A task cannot be created because of various reasons.
   *  (EvGrpcSpawnInteractiveTaskCb_ and EvGrpcExecuteTaskCb_)
   * @param release_resource If set to true, SlurmCtlXd will release the
   *  resource (mark the task status as REQUEUE) and requeue the task.
   */
  void EvActivateTaskStatusChange_(uint32_t task_id, ITask::Status new_status,
                                   std::optional<std::string> reason);

  /**
   * Send a signal to the process group to which the processes in
   *  ProcessInstance belongs.
   * This function ASSUMES that ALL processes belongs to the process group with
   *  the PGID set to the PID of the first process in this ProcessInstance.
   * @param signum the value of signal.
   * @return if the signal is sent successfully, kOk is returned.
   * if the task name doesn't exist, kNonExistent is returned.
   * if the signal is invalid, kInvalidParam is returned.
   * otherwise, kGenericFailure is returned.
   */
  static SlurmxErr KillProcessInstance_(const ProcessInstance* proc,
                                        int signum);

  // Note: the three maps below are NOT protected by any mutex.
  //  They should be modified in libev callbacks to avoid races.

  // Contains all the task that is running on this SlurmXd node.
  absl::flat_hash_map<uint32_t /*task id*/, std::unique_ptr<TaskInstance>>
      m_task_map_;

  // The two following maps are used as indexes and doesn't have the ownership
  // of underlying objects. A TaskInstance may contain more than one
  // ProcessInstance.
  absl::flat_hash_map<uint32_t /*pid*/, TaskInstance*> m_pid_task_map_;
  absl::flat_hash_map<uint32_t /*pid*/, ProcessInstance*> m_pid_proc_map_;

  util::CgroupManager& m_cg_mgr_;

  static void EvSigchldCb_(evutil_socket_t sig, short events, void* user_data);

  static void EvSigintCb_(evutil_socket_t sig, short events, void* user_data);

  static void EvGrpcExecuteTaskCb_(evutil_socket_t efd, short events,
                                   void* user_data);

  static void EvGrpcSpawnInteractiveTaskCb_(evutil_socket_t efd, short events,
                                            void* user_data);

  static void EvSubprocessReadCb_(struct bufferevent* bev, void* process);

  static void EvTaskStatusChangeCb_(evutil_socket_t efd, short events,
                                    void* user_data);

  static void EvTerminateTaskCb_(evutil_socket_t efd, short events,
                                 void* user_data);

  static void EvExitEventCb_(evutil_socket_t, short events, void* user_data);

  struct event_base* m_ev_base_;
  struct event* m_ev_sigchld_;

  // When this event is triggered, the TaskManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  struct event* m_ev_sigint_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_;

  // When a new task grpc message arrives, the grpc function (which
  //  runs in parallel) uses m_grpc_event_fd_ to inform the event
  //  loop thread and the event loop thread retrieves the message
  //  from m_grpc_reqs_. We use this to keep thread-safety.
  struct event* m_ev_grpc_interactive_task_;
  ConcurrentQueue<EvQueueGrpcInteractiveTask> m_grpc_interactive_task_queue_;

  // A custom event that handles the ExecuteTask RPC.
  struct event* m_ev_grpc_execute_task_;
  ConcurrentQueue<std::unique_ptr<TaskInstance>> m_grpc_execute_task_queue_;

  // When this event is triggered, the event loop will exit.
  struct event* m_ev_exit_event_;
  // Use eventfd here because the user-defined event sometimes can't be
  // triggered by event_activate(). We have no interest in finding out why
  // event_activate() can't work and just use eventfd as a reliable solution.
  int m_ev_exit_fd_;

  struct event* m_ev_task_status_change_;
  ConcurrentQueue<TaskStatusChange> m_task_status_change_queue_;

  struct event* m_ev_task_terminate_;
  ConcurrentQueue<EvQueueTaskTerminate> m_task_terminate_queue_;

  std::thread m_ev_loop_thread_;

  static inline TaskManager* m_instance_ptr_;
};
}  // namespace Xd

inline std::unique_ptr<Xd::TaskManager> g_task_mgr;