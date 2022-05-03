#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <grpc++/grpc++.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <chrono>
#include <memory>
#include <queue>
#include <thread>

#include "XdPublicDefs.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "slurmx/PublicHeader.h"

namespace Xd {

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using SlurmxGrpc::SlurmCtlXd;
using SlurmxGrpc::SlurmXdRegisterRequest;
using SlurmxGrpc::SlurmXdRegisterResult;

class CtlXdClient {
 public:
  CtlXdClient();

  ~CtlXdClient();

  void SetNodeId(XdNodeId node_id) { m_node_id_ = node_id; }

  /***
   * InitChannelAndStub the CtlXdClient to SlurmCtlXd.
   * @param server_address The "[Address]:[Port]" of SlurmCtlXd.
   * @return
   * If SlurmCtlXd is successfully connected, kOk is returned. <br>
   * If SlurmCtlXd cannot be connected within 3s, kConnectionTimeout is
   * returned.
   */
  void InitChannelAndStub(const std::string& server_address);

  void TaskStatusChangeAsync(TaskStatusChange&& task_status_change);

  [[nodiscard]] XdNodeId GetNodeId() const { return m_node_id_; };

 private:
  void AsyncSendThread_();

  absl::Mutex m_task_status_change_mtx_;

  std::list<TaskStatusChange> m_task_status_change_list_
      GUARDED_BY(m_task_status_change_mtx_);

  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_{false};

  std::shared_ptr<Channel> m_ctlxd_channel_;

  std::unique_ptr<SlurmCtlXd::Stub> m_stub_;

  XdNodeId m_node_id_;
};

}  // namespace Xd

inline std::unique_ptr<Xd::CtlXdClient> g_ctlxd_client;