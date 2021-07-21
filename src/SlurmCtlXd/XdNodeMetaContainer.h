#pragma once

#include <unordered_map>

#include "CtlXdPublicDefs.h"
#include "slurmx/Lock.h"
#include "slurmx/Pointer.h"

namespace CtlXd {

/**
 * All methods in this class is synchronized.
 */
class XdNodeMetaContainerInterface {
 public:
  using Mutex = slurmx::mutex;
  using LockGuard = slurmx::lock_guard;

  using MetasPtr = slurmx::ScopeExclusivePtr<Metas, Mutex>;

  virtual ~XdNodeMetaContainerInterface() = default;

  virtual void AddNode(const XdNodeStaticMeta& static_meta) = 0;

  virtual void DeleteNodeMeta(uint32_t index) = 0;

  /**
   * Provide a thread-safe way to access NodeMeta.
   * @return a ScopeExclusivePointerType class. During the initialization of
   * this type, the unique ownership of data pointed by data is acquired. If the
   * index does not exist, a nullptr is returned and no lock is held. Use bool()
   * to check it.
   */
  virtual MetasPtr GetMetasPtr() = 0;

 protected:
  XdNodeMetaContainerInterface() = default;
};

class XdNodeMetaContainerSimpleImpl : public XdNodeMetaContainerInterface {
 public:
  XdNodeMetaContainerSimpleImpl() = default;
  ~XdNodeMetaContainerSimpleImpl() = default;

  void AddNode(const XdNodeStaticMeta& static_meta) override;

  void DeleteNodeMeta(uint32_t index) override;

  MetasPtr GetMetasPtr() override;

 private:
  Metas metas_;
  Mutex mtx_;
};

}  // namespace CtlXd

inline std::unique_ptr<CtlXd::XdNodeMetaContainerInterface> g_meta_container;