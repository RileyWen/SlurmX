
/*
 * Utility functions for dealing with libcgroup.
 *
 * This is not meant to replace direct interaction with libcgroup, however
 * it provides some simple initialization and RAII wrappers.
 *
 */

#include <fmt/format.h>
#include <fmt/printf.h>
#include <pthread.h>

#include <array>
#include <boost/utility.hpp>
#include <cassert>
#include <map>
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
  MEMORY_LIMIT_BYTES,
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
        "memory.limit_in_bytes",
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
  ControllerFlags() : m_flags_(0u) {}

  explicit ControllerFlags(CgroupConstant::Controller controller)
      : m_flags_(1u << static_cast<uint64_t>(controller)) {}

  ControllerFlags(const ControllerFlags &val) = default;

  ControllerFlags operator|=(const ControllerFlags &rhs) {
    m_flags_ |= rhs.m_flags_;
    return *this;
  }

  ControllerFlags operator&=(const ControllerFlags &rhs) {
    m_flags_ &= rhs.m_flags_;
    return *this;
  }

  operator bool() const { return static_cast<bool>(m_flags_); }

 private:
  friend ControllerFlags operator|(const ControllerFlags &lhs,
                                   const ControllerFlags &rhs);
  friend ControllerFlags operator&(const ControllerFlags &lhs,
                                   const ControllerFlags &rhs);
  friend ControllerFlags operator|(const ControllerFlags &lhs,
                                   const CgroupConstant::Controller &rhs);
  friend ControllerFlags operator&(const ControllerFlags &lhs,
                                   const CgroupConstant::Controller &rhs);
  friend ControllerFlags operator|(const CgroupConstant::Controller &lhs,
                                   const CgroupConstant::Controller &rhs);
  uint64_t m_flags_;
};

inline ControllerFlags operator|(const ControllerFlags &lhs,
                                 const ControllerFlags &rhs) {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | rhs.m_flags_;
  return flags;
}

inline ControllerFlags operator&(const ControllerFlags &lhs,
                                 const ControllerFlags &rhs) {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & rhs.m_flags_;
  return flags;
}

inline ControllerFlags operator|(const ControllerFlags &lhs,
                                 const CgroupConstant::Controller &rhs) {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

inline ControllerFlags operator&(const ControllerFlags &lhs,
                                 const CgroupConstant::Controller &rhs) {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & (1u << static_cast<uint64_t>(rhs));
  return flags;
}

inline ControllerFlags operator|(const CgroupConstant::Controller &lhs,
                                 const CgroupConstant::Controller &rhs) {
  ControllerFlags flags;
  flags.m_flags_ =
      (1u << static_cast<uint64_t>(lhs)) | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

class Cgroup;  // Forward decl

class CgroupManager {
 public:
  static CgroupManager &getInstance();

  [[nodiscard]] bool isMounted(CgroupConstant::Controller controller) const {
    return bool(m_mounted_controllers_ & ControllerFlags{controller});
  }

  bool create_or_open(const std::string &cgroup_string,
             ControllerFlags preferred_controllers,
             ControllerFlags required_controllers, bool retrieve);
  bool destroy(const std::string &cgroup_path);

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

  struct CgroupInfo : boost::noncopyable {
    int ref_cnt;
    std::unique_ptr<Cgroup> cgroup_ptr;
  };
  // Ref-counting
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
