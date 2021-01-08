
/*
 * Utility functions for dealing with libcgroup.
 *
 * This is not meant to replace direct interaction with libcgroup, however
 * it provides some simple initialization and RAII wrappers.
 *
 */
#pragma once

#include <fmt/format.h>
#include <fmt/printf.h>
#include <pthread.h>

#include <array>
#include <boost/move/move.hpp>
#include <cassert>
#include <map>
#include <optional>
#include <string_view>

#include "libcgroup.h"

static pthread_mutex_t g_cgroups_mutex = PTHREAD_MUTEX_INITIALIZER;

namespace CgroupConstant {

enum class Controller : uint64_t {
  MEMORY_CONTROLLER = 0,
  CPUACCT_CONTROLLER,
  FREEZE_CONTROLLER,
  BLOCK_CONTROLLER,
  CPU_CONTROLLER,

  ControllerCount,
};

enum class ControllerFile : uint64_t {
  CPU_SHARES = 0,
  CPU_CFS_PERIOD_US,
  CPU_CFS_QUOTA_US,

  MEMORY_LIMIT_BYTES,
  MEMORY_MEMSW_LIMIT_IN_BYTES,
  MEMORY_SOFT_LIMIT_BYTES,

  BLOCKIO_WEIGHT,

  ControllerFileCount
};

namespace Internal {

constexpr std::array<std::string_view,
                     static_cast<size_t>(Controller::ControllerCount)>
    ControllerStringView{
        "memory", "cpuacct", "freezer", "blkio", "cpu",
    };

constexpr std::array<std::string_view,
                     static_cast<size_t>(ControllerFile::ControllerFileCount)>
    ControllerFileStringView{
        "cpu.shares",
        "cpu.cfs_period_us",
        "cpu.cfs_quota_us",

        "memory.limit_in_bytes",
        "memory.memsw.limit_in_bytes",
        "memory.soft_limit_in_bytes",

        "blkio.weight",
    };
}  // namespace Internal

constexpr std::string_view GetControllerStringView(Controller controller) {
  return Internal::ControllerStringView[static_cast<uint64_t>(controller)];
}

constexpr std::string_view GetControllerFileStringView(
    ControllerFile controller_file) {
  return Internal::ControllerFileStringView[static_cast<uint64_t>(
      controller_file)];
}

}  // namespace CgroupConstant

class ControllerFlags {
 public:
  ControllerFlags() noexcept : m_flags_(0u) {}

  explicit ControllerFlags(CgroupConstant::Controller controller) noexcept
      : m_flags_(1u << static_cast<uint64_t>(controller)) {}

  ControllerFlags(const ControllerFlags &val) noexcept = default;

  ControllerFlags operator|=(const ControllerFlags &rhs) noexcept {
    m_flags_ |= rhs.m_flags_;
    return *this;
  }

  ControllerFlags operator&=(const ControllerFlags &rhs) noexcept {
    m_flags_ &= rhs.m_flags_;
    return *this;
  }

  operator bool() const noexcept { return static_cast<bool>(m_flags_); }

  ControllerFlags operator~() const noexcept {
    ControllerFlags cf;
    cf.m_flags_ = ~m_flags_;
    return cf;
  }

 private:
  friend ControllerFlags operator|(const ControllerFlags &lhs,
                                   const ControllerFlags &rhs) noexcept;
  friend ControllerFlags operator&(const ControllerFlags &lhs,
                                   const ControllerFlags &rhs) noexcept;
  friend ControllerFlags operator|(
      const ControllerFlags &lhs,
      const CgroupConstant::Controller &rhs) noexcept;
  friend ControllerFlags operator&(
      const ControllerFlags &lhs,
      const CgroupConstant::Controller &rhs) noexcept;
  friend ControllerFlags operator|(
      const CgroupConstant::Controller &lhs,
      const CgroupConstant::Controller &rhs) noexcept;
  uint64_t m_flags_;
};

inline ControllerFlags operator|(const ControllerFlags &lhs,
                                 const ControllerFlags &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | rhs.m_flags_;
  return flags;
}

inline ControllerFlags operator&(const ControllerFlags &lhs,
                                 const ControllerFlags &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & rhs.m_flags_;
  return flags;
}

inline ControllerFlags operator|(
    const ControllerFlags &lhs,
    const CgroupConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

inline ControllerFlags operator&(
    const ControllerFlags &lhs,
    const CgroupConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & (1u << static_cast<uint64_t>(rhs));
  return flags;
}

inline ControllerFlags operator|(
    const CgroupConstant::Controller &lhs,
    const CgroupConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ =
      (1u << static_cast<uint64_t>(lhs)) | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

const ControllerFlags NO_CONTROLLER_FLAG{};

// In many distributions, 'cpu' and 'cpuacct' are mounted together. 'cpu'
//  and 'cpuacct' both point to a single 'cpu,cpuacct' account. libcgroup
//  handles this for us and no additional care needs to be take.
const ControllerFlags ALL_CONTROLLER_FLAG = (~NO_CONTROLLER_FLAG);

class Cgroup;  // Forward decl

class CgroupManager {
 private:
  struct CgroupInfo {
    CgroupInfo() : ref_cnt(0){};

    CgroupInfo(CgroupInfo &&) = default;
    CgroupInfo &operator=(CgroupInfo &&) = default;

    int ref_cnt;
    std::unique_ptr<Cgroup> cgroup_ptr;

   private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(CgroupInfo);
  };

 public:
  using CgroupInfoCRefWrapper = std::reference_wrapper<const CgroupInfo>;

  static CgroupManager &getInstance();

  [[nodiscard]] bool isMounted(CgroupConstant::Controller controller) const {
    return bool(m_mounted_controllers_ & ControllerFlags{controller});
  }

  bool create_or_open(const std::string &cgroup_string,
                      ControllerFlags preferred_controllers,
                      ControllerFlags required_controllers, bool retrieve);
  bool destroy(const std::string &cgroup_path);

  std::optional<CgroupInfoCRefWrapper> find_cgroup(
      const std::string &cgroup_path);

  bool migrate_proc_to_cgroup(pid_t pid, const std::string &cgroup_path);

 private:
  CgroupManager();
  CgroupManager(const CgroupManager &);
  CgroupManager &operator=(const CgroupManager &);

  int initialize();

  int initialize_controller(struct cgroup &cgroup,
                            CgroupConstant::Controller controller,
                            bool required, bool has_cgroup,
                            bool &changed_cgroup) const;

  // ControllerFlags m_cgroup_mounts;

  ControllerFlags m_mounted_controllers_;

  static CgroupManager *m_singleton;
  std::map<std::string, CgroupInfo> m_cgroup_info_;

  class MutexGuard {
   public:
    MutexGuard(pthread_mutex_t &mutex) : m_mutex(mutex) {
      pthread_mutex_lock(&m_mutex);
    }
    ~MutexGuard() { pthread_mutex_unlock(&m_mutex); }

   private:
    pthread_mutex_t &m_mutex;
  };

  static MutexGuard getGuard() { return MutexGuard(g_cgroups_mutex); }
};

class Cgroup {
 public:
  Cgroup() : m_cgroup(nullptr) {}
  ~Cgroup();

  void destroy();

  struct cgroup &getCgroup() {
    if (isValid()) {
      return *m_cgroup;
    }
    fmt::print(stderr, "Accessing invalid cgroup.");
    return *m_cgroup;
  }
  const std::string &getCgroupString() { return m_cgroup_string; };

  // Using the zombie object pattern as exceptions are not available.
  bool isValid() { return m_cgroup != NULL; }

 private:
  std::string m_cgroup_string;
  struct cgroup *m_cgroup;

 protected:
  void setCgroupString(const std::string &cgroup_string) {
    m_cgroup_string = cgroup_string;
  };
  void setCgroup(struct cgroup &cgroup);

  friend class CgroupManager;
};
