#pragma once

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <evrpc.h>
#include <grpc++/grpc++.h>
#include <sys/eventfd.h>
#include <sys/signal.h>
#include <sys/wait.h>

#include <atomic>
#include <boost/algorithm/string.hpp>
#include <forward_list>
#include <functional>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "AnonymousPipe.h"
#include "PublicHeader.h"
#include "cgroup.linux.h"
#include "concurrentqueue/concurrentqueue.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "spdlog/spdlog.h"

namespace Xd {

// This structure is passed as an argument of AddTaskAsync.
struct TaskInitInfo {
  std::string name;

  std::string executive_path;
  std::forward_list<std::string> arguments;

  Cgroup::CgroupLimit cg_limit;

  /***
   * The callback function called when a task writes to stdout or stderr.
   * @param[in] buf a slice of output buffer.
   */
  std::function<void(std::string&& buf)> output_callback;

  /***
   * The callback function called when a task is finished.
   * @param[in] bool true if the task is terminated by a signal, false
   * otherwise.
   * @param[in] int the number of signal if bool is true, the return value
   * otherwise.
   */
  std::function<void(bool, int)> finish_callback;
};

// Todo: Task may consists of multiple subtasks
struct Task {
  TaskInitInfo init_info;

  // fields below are types of Task runtime information

  // The pid of the root process of a task.
  // Also the pgid of all processes spawned from the root process.
  // When a signal is sent to a task, it's sent to this pgid.
  pid_t root_pid = {};

  // The cgroup name that restrains the Task.
  std::string cg_path;

  // The underlying event that handles the output of the task.
  struct bufferevent* ev_buf_event;
};

// Used to get the result of new task appending from event loop thread
struct grpc_resp_new_task_t {
  SlurmxErr err;
};

// Used to pass new task from grpc thread to event loop thread
struct grpc_req_new_task_t {
  TaskInitInfo task_init_info;
  std::promise<grpc_resp_new_task_t> resp_promise;
};

/***
 * The class that manages all tasks and handles interrupts.
 * SIGINT and SIGCHLD are processed in TaskManager.
 * Especially, outside caller can use SetSigintCallback() to
 * set the callback when SIGINT is triggered.
 */
class TaskManager {
 public:
  TaskManager();

  ~TaskManager();

  /***
   * This function is thread-safe.
   * @param task_info The initialization information of a task.
   * @return
   * If the task is added successfully, kOk is returned. <br>
   * If the task name exists, kExistingTask is returned. <br>
   * If SIGINT is triggered and the TaskManager is stopping, kStop is returned.
   */
  SlurmxErr AddTaskAsync(TaskInitInfo&& task_info);

  // Wait internal libevent base loop to exit...
  void Wait();

  /***
   * Send a signal to the task (which is a process group).
   * @param task_name the name of the task.
   * @param signum the value of signal.
   * @return if the signal is sent successfully, kOk is returned.
   * if the task name doesn't exist, kNonExistent is returned.
   * if the signal is invalid, kInvalidParam is returned.
   * otherwise, kGenericFailure is returned.
   */
  SlurmxErr Kill(const std::string& task_name, int signum);

  /***
   * Set the callback function will be called when SIGINT is triggered.
   * This function is not thread-safe.
   * @param cb the callback function.
   */
  void SetSigintCallback(std::function<void()> cb);

 private:
  static std::string CgroupStrByPID(pid_t pid);

  static inline TaskManager* m_instance_ptr_;

  std::optional<const Task*> FindTaskByName(const std::string& task_name);

  // Note: the two maps below are NOT protected by any mutex.
  //  They should be modified in libev callbacks to avoid races.

  // Users submit tasks by name.
  std::unordered_map<std::string, std::unique_ptr<Task>> m_name_to_task_map_;

  // But the system tracks process by pid. We need to maintain
  //  a mapping from pid to task name.
  std::unordered_map<pid_t, std::string> m_pid_to_name_map_;

  Cgroup::CgroupManager& m_cg_mgr_;

  static void ev_sigchld_cb_(evutil_socket_t sig, short events,
                             void* user_data);

  static void ev_sigint_cb_(evutil_socket_t sig, short events, void* user_data);

  static void ev_grpc_event_cb_(evutil_socket_t, short events, void* user_data);

  static void ev_subprocess_read_cb_(struct bufferevent* bev, void* pid_);

  struct sigchld_info_t {
    pid_t pid;

    bool is_terminated_by_signal;

    int value;
  };

  struct event_base* m_ev_base_;
  struct event* m_ev_sigchld_;

  // When this event is triggered, the TaskManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  struct event* m_ev_sigint_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered, this variable is set to true.
  // Then, AddTaskAsyncMethod will not accept any more new tasks
  // and ev_sigchld_cb_ will stop the event loop when there is
  // no task running.
  std::atomic_bool m_is_ending_now_;

  // When a new task grpc message arrives, the grpc function (which
  //  runs in parallel) uses m_grpc_event_fd_ to inform the event
  //  loop thread and the event loop thread retrieves the message
  //  from m_grpc_reqs_. We use this to keep thread-safety.
  struct event* m_ev_grpc_event_;
  int m_grpc_event_fd_;
  moodycamel::ConcurrentQueue<grpc_req_new_task_t> m_gprc_new_task_queue_;

  std::thread m_ev_loop_thread_;
};

}  // namespace Xd

inline std::unique_ptr<Xd::TaskManager> g_task_mgr;