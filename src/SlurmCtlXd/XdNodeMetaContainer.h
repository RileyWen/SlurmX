#pragma once

#include <absl/container/flat_hash_map.h>

#include <unordered_map>

#include "CtlXdPublicDefs.h"
#include "slurmx/Lock.h"
#include "slurmx/Pointer.h"

namespace CtlXd {

/**
 * All public methods in this class is thread-safe.
 */
class XdNodeMetaContainerInterface {
 public:
  using Mutex = util::recursive_mutex;
  using LockGuard = util::recursive_lock_guard;

  using AllPartitionsMetaMap =
      absl::flat_hash_map<uint32_t /*partition id*/, PartitionMetas>;

  using AllPartitionsMetaMapPtr =
      util::ScopeExclusivePtr<AllPartitionsMetaMap, Mutex>;
  using PartitionMetasPtr = util::ScopeExclusivePtr<PartitionMetas, Mutex>;
  using NodeMetaPtr = util::ScopeExclusivePtr<XdNodeMeta, Mutex>;

  virtual ~XdNodeMetaContainerInterface() = default;

  virtual void NodeUp(const XdNodeId& node_id) = 0;

  virtual void NodeDown(XdNodeId node_id) = 0;

  virtual void InitFromConfig(const Config& config) = 0;

  /**
   * Check whether partition exists.
   */
  virtual bool PartitionExists(const std::string& partition_name) = 0;

  virtual bool CheckNodeAllowed(const std::string& hostname) = 0;

  virtual SlurmxGrpc::QueryNodeInfoReply* QueryAllNodeInfo() = 0;

  virtual SlurmxGrpc::QueryNodeInfoReply* QueryNodeInfo(
      const std::string& node_name) = 0;

  virtual SlurmxGrpc::QueryPartitionInfoReply* QueryAllPartitionInfo() = 0;

  virtual SlurmxGrpc::QueryPartitionInfoReply* QueryPartitionInfo(
      const std::string& partition_name) = 0;

  virtual bool GetNodeId(const std::string& hostname, XdNodeId* node_id) = 0;

  /**
   * @return The partition id. If partition name doesn't exist, a new partition
   * id will be allocated to this partition name.
   */
  virtual bool GetPartitionId(const std::string& partition_name,
                              uint32_t* partition_id) = 0;

  virtual void MallocResourceFromNode(XdNodeId node_id, uint32_t task_id,
                                      const Resources& resources) = 0;
  virtual void FreeResourceFromNode(XdNodeId node_id, uint32_t task_id) = 0;

  /**
   * Provide a thread-safe way to access NodeMeta.
   * @return a ScopeExclusivePointerType class. During the initialization of
   * this type, the unique ownership of data pointed by data is acquired. If the
   * partition does not exist, a nullptr is returned and no lock is held. Use
   * bool() to check it.
   */
  virtual PartitionMetasPtr GetPartitionMetasPtr(uint32_t partition_id) = 0;

  virtual NodeMetaPtr GetNodeMetaPtr(XdNodeId node_id) = 0;

  virtual AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() = 0;

 protected:
  XdNodeMetaContainerInterface() = default;
};

class XdNodeMetaContainerSimpleImpl final
    : public XdNodeMetaContainerInterface {
 public:
  XdNodeMetaContainerSimpleImpl() = default;
  ~XdNodeMetaContainerSimpleImpl() override = default;

  void InitFromConfig(const Config& config) override;

  SlurmxGrpc::QueryNodeInfoReply* QueryAllNodeInfo() override;

  SlurmxGrpc::QueryNodeInfoReply* QueryNodeInfo(
      const std::string& node_name) override;

  SlurmxGrpc::QueryPartitionInfoReply* QueryAllPartitionInfo() override;

  SlurmxGrpc::QueryPartitionInfoReply* QueryPartitionInfo(
      const std::string& partition_name) override;

  bool GetNodeId(const std::string& hostname, XdNodeId* node_id) override;

  void NodeUp(const XdNodeId& node_id) override;

  void NodeDown(XdNodeId node_id) override;

  PartitionMetasPtr GetPartitionMetasPtr(uint32_t partition_id) override;

  NodeMetaPtr GetNodeMetaPtr(XdNodeId node_id) override;

  AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() override;

  bool PartitionExists(const std::string& partition_name) override;

  bool CheckNodeAllowed(const std::string& hostname) override;

  bool GetPartitionId(const std::string& partition_name,
                      uint32_t* partition_id) override;

  void MallocResourceFromNode(XdNodeId node_id, uint32_t task_id,
                              const Resources& resources) override;

  void FreeResourceFromNode(XdNodeId node_id, uint32_t task_id) override;

 private:
  AllPartitionsMetaMap partition_metas_map_;

  absl::flat_hash_map<std::string /*partition name*/, uint32_t /*partition id*/>
      partition_name_id_map_;

  absl::flat_hash_map<std::string /*node hostname*/, uint32_t /*partition id*/>
      node_hostname_part_id_map_;

  absl::flat_hash_map<std::pair<uint32_t, std::string /*hostname*/>,
                      uint32_t /*node index in a partition*/>
      part_id_host_index_map_;

  Mutex mtx_;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::XdNodeMetaContainerInterface> g_meta_container;