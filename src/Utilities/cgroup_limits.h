#include "cgroup.linux.h"

class CgroupLimits {
 public:
  explicit CgroupLimits(std::string cgroup_path);

  inline bool set_memory_limit_bytes(uint64_t memory_bytes);
  inline bool set_memory_soft_limit_bytes(uint64_t memory_bytes);
  inline bool set_cpu_shares(uint64_t share);
  inline bool set_blockio_weight(uint64_t weight);

 private:
  bool set_controller_value_(CgroupConstant::Controller controller,
                             CgroupConstant::ControllerFile controller_file,
                             uint64_t value);

  const std::string m_cgroup_string;
  Cgroup m_cgroup;
};
