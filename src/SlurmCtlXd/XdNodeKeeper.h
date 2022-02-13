#pragma once

#include <grpc++/alarm.h>
#include <grpc++/completion_queue.h>
#include <grpc++/grpc++.h>

#include <boost/dynamic_bitset.hpp>
#include <boost/pool/object_pool.hpp>
#include <boost/uuid/uuid.hpp>
#include <functional>
#include <future>
#include <memory>
#include <thread>

#include "CtlXdPublicDefs.h"
#include "protos/slurmx.grpc.pb.h"
#include "protos/slurmx.pb.h"
#include "slurmx/Lock.h"
#include "slurmx/PublicHeader.h"

namespace CtlXd {

class XdNodeKeeper;

struct RegisterNodeResult {
  std::optional<XdNodeId> node_id;
};

/**
 * A class that encapsulate the detail of the underlying gRPC stub.
 */
class XdNodeStub {
 public:
  XdNodeStub();

  ~XdNodeStub();

  SlurmxErr ExecuteTask(const TaskInCtlXd *task);

  SlurmxErr TerminateTask(uint32_t task_id);

  void *GetNodeData() { return m_data_; };

  void SetNodeData(void *data) { m_data_ = data; }

  bool Invalid() const { return m_invalid_; }

 private:
  uint32_t m_slot_offset_;

  grpc_connectivity_state m_prev_channel_state_;
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<SlurmxGrpc::SlurmXd::Stub> m_stub_;

  // Set if underlying gRPC is down.
  bool m_invalid_;

  uint32_t m_maximum_retry_times_;
  uint32_t m_failure_retry_times_;

  XdNodeId m_node_id_;

  void *m_data_;

  // void* parameter is m_data_. Used to free m_data_ when XdNodeStub is being
  // destructed.
  std::function<void(void *)> m_clean_up_cb_;

  friend class XdNodeKeeper;
};

class XdNodeKeeper {
 public:
  XdNodeKeeper();

  ~XdNodeKeeper();

  /**
   * Request to register a new Xd node. Thread-safe.
   * @param node_addr The address passed to grpc::CreateChannel().
   * @param data The user-specified data pointer which will be passed as a
   * parameter from all callbacks.
   * @param clean_up_cb Called when the underlying Xd node structure is
   * destroyed. The parameter `void *` will be `node_data`.
   * @return A future of RegisterNodeResult. When the backward connection to the
   * new Xd node succeeds or fails, the future is set, in which the node index
   * option is set (succeed) or is null (fail).
   */
  std::future<RegisterNodeResult> RegisterXdNode(
      const std::string &node_addr, XdNodeId node_id, void *data,
      std::function<void(void *)> clean_up_cb);

  uint32_t AvailableNodeCount();

  bool XdNodeValid(uint32_t index);

  /**
   * Get the pointer to XdNodeStub.
   * @param node_id the index of XdNodeStub
   * @return nullptr if index points to an invalid slot, the pointer to
   * XdNodeStub otherwise.
   * @attention It's ok to return the pointer of XdNodeStub directly. The
   * XdNodeStub will not be freed before the NodeIsDown() callback returns. The
   * callback registerer should do necessary synchronization to clean up all the
   * usage of the XdNodeStub pointer before NodeIsDown() returns.
   */
  XdNodeStub *GetXdStub(XdNodeId node_id);

  void SetNodeIsUpCb(std::function<void(XdNodeId, void *)> cb);

  void SetNodeIsDownCb(std::function<void(XdNodeId, void *)> cb);

  void SetNodeIsTempDownCb(std::function<void(XdNodeId, void *)> cb);

  void SetNodeRecFromTempFailureCb(std::function<void(XdNodeId, void *)> cb);

 private:
  struct InitializingXdTagData {
    std::unique_ptr<XdNodeStub> xd;
    std::promise<RegisterNodeResult> register_result;
  };

  struct CqTag {
    enum Type { kInitializingXd, kEstablishedXd };
    Type type;
    void *data;
  };

  CqTag *InitXdStateMachine_(InitializingXdTagData *tag_data,
                             grpc_connectivity_state new_state);
  CqTag *EstablishedXdStateMachine_(XdNodeStub *xd,
                                    grpc_connectivity_state new_state);

  void StateMonitorThreadFunc_();

  std::function<void(XdNodeId, void *)> m_node_is_up_cb_;
  std::function<void(XdNodeId, void *)> m_node_is_temp_down_cb_;
  std::function<void(XdNodeId, void *)> m_node_rec_from_temp_failure_cb_;

  // Guarantee that the Xd node will not be freed before this callback is
  // called.
  std::function<void(XdNodeId, void *)> m_node_is_down_cb_;

  util::mutex m_tag_pool_mtx_;

  // Must be declared previous to any grpc::CompletionQueue, so it can be
  // constructed before any CompletionQueue and be destructed after any
  // CompletionQueue.
  boost::object_pool<CqTag> m_tag_pool_;

  // Protect m_node_vec_, m_node_id_slot_offset_map_ and m_empty_slot_bitset_.
  util::mutex m_node_mtx_;

  // Todo: Change to std::shared_ptr. GRPC has sophisticated error handling
  //  mechanism. So it's ok to access the stub when the Xd node is down. What
  //  should be avoided is null pointer accessing.
  // Contains connection-established nodes only.
  std::vector<std::unique_ptr<XdNodeStub>> m_node_vec_;

  // Used to track the empty slots in m_node_vec_. We can use find_first() to
  // locate the first empty slot.
  boost::dynamic_bitset<> m_empty_slot_bitset_;

  std::unordered_map<XdNodeId, uint32_t, XdNodeId::Hash>
      m_node_id_slot_offset_map_;

  // Protect m_alive_xd_bitset_
  util::rw_mutex m_alive_xd_rw_mtx_;

  // If bit n is set, the xd client n is available to send grpc. (underlying
  // grpc channel state is GRPC_CHANNEL_READY).
  boost::dynamic_bitset<> m_alive_xd_bitset_;

  grpc::CompletionQueue m_cq_;
  util::mutex m_cq_mtx_;
  bool m_cq_closed_;

  std::thread m_cq_thread_;
};

inline std::unique_ptr<XdNodeKeeper> g_node_keeper;

}  // namespace CtlXd