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
#include <boost/multi_index/global_fun.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
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
  /// \b name should NOT be modified after being assigned! It's used as an
  /// index.
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

  /// The pid of the root process of a task.
  /// Also the pgid of all processes spawned from the root process.
  /// When a signal is sent to a task, it's sent to this pgid.
  /// \b root_pid should NOT be modified after being assigned! It's used as an
  /// index.
  pid_t root_pid = {};

  // The cgroup name that restrains the Task.
  std::string cg_path;

  // The underlying event that handles the output of the task.
  struct bufferevent* ev_buf_event;
};

namespace Internal {

/*
 * The implementation of TaskMultiIndexSet.
 */

using boost::multi_index::indexed_by;
using boost::multi_index::ordered_unique;
using boost::multi_index::tag;

struct TaskName {};
struct Pid {};

struct TaskPtrWrapper {
  const std::string& name() const { return p_->init_info.name; }
  pid_t pid() const { return p_->root_pid; }

  std::unique_ptr<Task> p_;
};

// clang-format off
typedef boost::multi_index_container<
    TaskPtrWrapper,
    indexed_by<
        ordered_unique<
          tag<TaskName>,
          BOOST_MULTI_INDEX_CONST_MEM_FUN(TaskPtrWrapper, const std::string&, name)
        >,
        ordered_unique<
          tag<Pid>,
          BOOST_MULTI_INDEX_CONST_MEM_FUN(TaskPtrWrapper, pid_t, pid)
        >
    >
> TaskMultiIndexSetInternal;
// clang-format on

}  // namespace Internal

class TaskMultiIndexSet {
 public:
  using iterator_type =
      Internal::TaskMultiIndexSetInternal::index_iterator<Internal::Pid>::type;

  void Insert(std::unique_ptr<Task>&& task);

  const Task* FindByName(const std::string& name);

  const Task* FindByPid(pid_t pid);

  bool Empty() { return task_set_.empty(); }

  void EraseByName(const std::string& name);

  size_t CountByName(const std::string& name);

  // Adoption for range-based for.
  iterator_type begin();
  iterator_type end();

 private:
  Internal::TaskMultiIndexSetInternal task_set_;
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

  // Ask TaskManager to stop its event loop.
  void Shutdown();

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
  TaskMultiIndexSet m_task_set_;

  Cgroup::CgroupManager& m_cg_mgr_;

  static void EvSigchldCb_(evutil_socket_t sig, short events, void* user_data);

  static void EvSigintCb_(evutil_socket_t sig, short events, void* user_data);

  static void EvGrpcEventCb_(evutil_socket_t, short events, void* user_data);

  static void EvSubprocessReadCb_(struct bufferevent* bev, void* pid_);

  static void EvExitEventCb_(evutil_socket_t, short events, void* user_data);

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

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_;

  // When a new task grpc message arrives, the grpc function (which
  //  runs in parallel) uses m_grpc_event_fd_ to inform the event
  //  loop thread and the event loop thread retrieves the message
  //  from m_grpc_reqs_. We use this to keep thread-safety.
  struct event* m_ev_grpc_event_;
  int m_grpc_event_fd_;
  moodycamel::ConcurrentQueue<grpc_req_new_task_t> m_gprc_new_task_queue_;

  // When this event is triggered, the event loop will exit.
  struct event* m_ev_exit_event_;
  // Use eventfd here because the user-defined event sometimes can't be
  // triggered by event_activate(). We have no interest in finding out why
  // event_activate() can't work and just use eventfd as a reliable solution.
  int m_ev_exit_fd_;

  std::thread m_ev_loop_thread_;
};
}  // namespace Xd

inline std::unique_ptr<Xd::TaskManager> g_task_mgr;