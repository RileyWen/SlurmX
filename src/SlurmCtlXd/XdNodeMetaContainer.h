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

  virtual void AddNode(const XdNodeStaticMeta& static_meta) = 0;

  virtual void DeleteNodeMeta(XdNodeId node_id) = 0;

  /**
   * Check whether partition exists.
   */
  virtual bool PartitionExists(const std::string& partition_name) = 0;

  /**
   * @return The partition id. If partition name doesn't exist, a new partition
   * id will be allocated to this partition name.
   */
  virtual bool GetPartitionId(const std::string& partition_name,
                              uint32_t* partition_id) = 0;

  virtual uint32_t AllocPartitionId(const std::string& partition_name) = 0;

  /**
   * Free all the structures of a partition if this partition is empty.
   * Otherwise, no operation is performed.
   * @param partition_id
   */
  [[deprecated]] virtual void TryReleasePartition(uint32_t partition_id) = 0;

  /**
   * @return Next unique node index in this partition.
   * Caller Must make sure that the partition exists when calling this function.
   * If partition id does not exist, 0xFFFFFFFF is returned.
   */
  virtual uint32_t AllocNodeIndexInPartition(uint32_t partition_id) = 0;

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

  virtual uint32_t GetNextPartitionSeq_() = 0;
};

class XdNodeMetaContainerSimpleImpl final
    : public XdNodeMetaContainerInterface {
 public:
  XdNodeMetaContainerSimpleImpl() = default;
  ~XdNodeMetaContainerSimpleImpl() override = default;

  void AddNode(const XdNodeStaticMeta& static_meta) override;

  void DeleteNodeMeta(XdNodeId node_id) override;

  PartitionMetasPtr GetPartitionMetasPtr(uint32_t partition_id) override;

  NodeMetaPtr GetNodeMetaPtr(XdNodeId node_id) override;

  AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() override;

  bool PartitionExists(const std::string& partition_name) override;

  bool GetPartitionId(const std::string& partition_name,
                      uint32_t* partition_id) override;

  uint32_t AllocPartitionId(const std::string& partition_name) override;

  void TryReleasePartition(uint32_t partition_id) override;

  uint32_t AllocNodeIndexInPartition(uint32_t partition_id) override;

  void MallocResourceFromNode(XdNodeId node_id, uint32_t task_id,
                              const Resources& resources) override;

  void FreeResourceFromNode(XdNodeId node_id, uint32_t task_id) override;

 private:
  /**
   * This function is not thread-safe.
   */
  void TryReleasePartition_(uint32_t partition_id);

  /**
   * This function is not thread-safe.
   * @return next unique partition sequence number.
   */
  uint32_t GetNextPartitionSeq_() override;

  AllPartitionsMetaMap partition_metas_map_;

  absl::flat_hash_map<std::string /*partition name*/, uint32_t /*partition id*/>
      partition_name_id_map_;
  uint32_t partition_seq_ = 0;

  Mutex mtx_;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::XdNodeMetaContainerInterface> g_meta_container;