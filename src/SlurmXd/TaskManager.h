#pragma once

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <grpc++/grpc++.h>
#include <sys/eventfd.h>
#include <sys/signal.h>
#include <sys/wait.h>

#include <atomic>
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

// This structure is passed as an argument of AddTaskAsync.
struct TaskInitInfo {
  std::string name;

  std::string executive_path;
  std::forward_list<std::string> arguments;

  CgroupLimit cg_limit;

  /// @param[in] buf a slice of output buffer.
  std::function<void(std::string&& buf)> output_callback;

  /// @param[in] bool true if the task is terminated by a signal, false
  /// otherwise.
  /// @param[in] int the number of signal if bool is true, the return value
  /// otherwise.
  std::function<void(bool, int)> finish_callback;
};

using TaskInitInfoRef = std::reference_wrapper<TaskInitInfo>;
using TaskInitInfoCRef = std::reference_wrapper<const TaskInitInfo>;

// Todo: Task may consists of multiple subtasks
struct Task {
  TaskInitInfo init_info;

  // Task runtime info
  pid_t root_pid = {};
  std::string cg_path;
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

class TaskManager {
  // TODO: 1. Add process group supporting.
 public:
  static TaskManager& GetInstance() {
    static TaskManager singleton;
    return singleton;
  }

  std::optional<TaskInitInfoCRef> FindTaskInfoByName(
      const std::string& task_name);

 private:
  static std::string CgroupStrByPID(pid_t pid);

  TaskManager();

  ~TaskManager();

  // Users submit tasks by name.
  std::unordered_map<std::string, std::unique_ptr<Task>> m_name_to_task_map_;

  // But the system tracks process by pid. We need to maintain
  //  a mapping from pid to task name.
  std::unordered_map<pid_t, std::string> m_pid_to_name_map_;

  CgroupManager& m_cg_mgr_;

  // Functions and members for libevent {
  static inline TaskManager* m_instance_ptr_;

  // Runs in the loop thread
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
  struct event* m_ev_sigint_;

  // When a new task grpc message arrives, the grpc function (which
  //  runs in parallel) uses m_grpc_event_fd_ to inform the event
  //  loop thread and the event loop thread retrieves the message
  //  from m_grpc_reqs_. We use this to keep thread-safety.
  struct event* m_ev_grpc_event_;
  int m_grpc_event_fd_;

  moodycamel::ConcurrentQueue<grpc_req_new_task_t> m_gprc_new_task_queue_;

  std::thread m_ev_loop_thread_;

 public:
  // This function is thread-safe.
  SlurmxErr AddTaskAsync(TaskInitInfo&& task_info);

  // Wait internal libevent base loop to exit...
  void Wait();
};