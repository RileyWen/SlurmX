#pragma once

#include <sys/eventfd.h>
#include <sys/signal.h>
#include <sys/wait.h>

#include <atomic>
#include <forward_list>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "AnonymousPipe.h"
#include "cgroup.linux.h"
#include "concurrentqueue/concurrentqueue.h"
#include "event2/bufferevent.h"
#include "event2/event.h"
#include "event2/util.h"
#include "spdlog/spdlog.h"

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
  struct bufferevent* ev_buf_event;
};

struct grpc_resp_t {
  bool status;
  std::optional<std::string> reason;
};

struct grpc_req_t {
  TaskInfo task_info;
  std::promise<grpc_resp_t> resp_promise;
};

class TaskManager {
  // TODO: 1. Add SIGCHLD handling
  //       2. Add process group supporting.
 public:
  static TaskManager& GetInstance() {
    static TaskManager singleton;
    return singleton;
  }

  std::optional<TaskInfoCRef> FindTaskInfoByName(const std::string& task_name);

 private:
  static std::string CgroupStrByPID(pid_t pid);

  TaskManager();

  ~TaskManager();

  std::unordered_map<pid_t, std::string> m_pid_to_name_map_;
  std::unordered_map<std::string, Task> m_name_to_task_map_;
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

    enum struct return_type_t { RET_VAL, SIGNAL };
    return_type_t return_type;

    union status_t {
      int ret_val;
      int signum;
    };
    status_t status;
  };

  struct event_base* m_ev_base_;
  struct event* m_ev_sigchld_;
  struct event* m_ev_sigint_;

  struct event* m_ev_grpc_event_;
  int m_grpc_event_fd_;

  moodycamel::ConcurrentQueue<grpc_req_t> m_gprc_reqs_;

  std::thread m_ev_loop_thread_;

 public:
  // This function is thread-safe.
  std::future<grpc_resp_t> AddTaskAsync(TaskInfo&& task_info);

  // Wait internal libevent base loop to exit...
  void Wait();
};