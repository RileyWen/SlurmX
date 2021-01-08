#include "cgroup_limits.h"

#include <utility>

CgroupLimits::CgroupLimits(std::string cgroup_path)
    : m_cgroup_string(std::move(cgroup_path)) {}

bool CgroupLimits::set_memory_soft_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_SOFT_LIMIT_BYTES, mem_bytes);
}

bool CgroupLimits::set_memory_sw_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_MEMSW_LIMIT_IN_BYTES, mem_bytes);
}

bool CgroupLimits::set_memory_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_LIMIT_BYTES, mem_bytes);
}

bool CgroupLimits::set_cpu_shares(uint64_t shares) {
  return set_controller_value_(CgroupConstant::Controller::CPU_CONTROLLER,
                               CgroupConstant::ControllerFile::CPU_SHARES,
                               shares);
}
bool CgroupLimits::set_cpu_core_limit(uint64_t core_num) {
  constexpr uint32_t base = 1000'000;

  bool ret;
  ret = set_controller_value_(CgroupConstant::Controller::CPU_CONTROLLER,
                              CgroupConstant::ControllerFile::CPU_CFS_QUOTA_US,
                              base * core_num);
  ret &= set_controller_value_(CgroupConstant::Controller::CPU_CONTROLLER,
                               CgroupConstant::ControllerFile::CPU_CFS_PERIOD_US,
                               base);

  return ret;
}

bool CgroupLimits::set_blockio_weight(uint64_t weight) {
  return set_controller_value_(CgroupConstant::Controller::BLOCK_CONTROLLER,
                               CgroupConstant::ControllerFile::BLOCKIO_WEIGHT,
                               weight);
}

bool CgroupLimits::set_controller_value_(
    CgroupConstant::Controller controller,
    CgroupConstant::ControllerFile controller_file, uint64_t value) {
  CgroupManager &cm = CgroupManager::getInstance();

  if (!cm.isMounted(controller)) {
    fmt::print(stderr, "Unable to set {} because cgroup {} is not mounted.\n",
               CgroupConstant::GetControllerFileStringView(controller_file),
               CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  std::optional<CgroupManager::CgroupInfoCRefWrapper> cg_info_opt =
      cm.find_cgroup(m_cgroup_string);
  if (!cg_info_opt.has_value()) {
    fmt::print(stderr, "CgroupManager can't find cgroup {}.\n",
               m_cgroup_string);

    return false;
  }

  int err;
  struct cgroup *cg = &cg_info_opt.value().get().cgroup_ptr->getCgroup();
  struct cgroup_controller *cg_controller;

  if ((cg_controller = cgroup_get_controller(
           cg, CgroupConstant::GetControllerStringView(controller).data())) ==
      nullptr) {
    fmt::print(stderr, "Unable to get cgroup {} controller for {}.\n",
               CgroupConstant::GetControllerStringView(controller),
               m_cgroup_string.c_str());
    return false;
  }

  if ((err = cgroup_set_value_uint64(
           cg_controller,
           CgroupConstant::GetControllerFileStringView(controller_file).data(),
           value))) {
    fmt::fprintf(stderr, "Unable to set block IO weight for %s: %u %s\n",
                 m_cgroup_string.c_str(), err, cgroup_strerror(err));
    return false;
  }

  // Commit cgroup modifications.
  if ((err = cgroup_modify_cgroup(cg))) {
    fmt::print(stderr, "Unable to commit {} for cgroup {}: {} {}\n",
               CgroupConstant::GetControllerFileStringView(controller_file),
               m_cgroup_string.c_str(), err, cgroup_strerror(err));
    return false;
  }

  return true;
}
