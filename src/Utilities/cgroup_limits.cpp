#include "cgroup_limits.h"

#include <utility>

CgroupLimits::CgroupLimits(std::string cgroup_path)
    : m_cgroup_string(std::move(cgroup_path)) {
  using CgroupConstant::Controller;

  ControllerFlags NO_CONTROLLER;

  CgroupManager::getInstance().create_or_open(m_cgroup_string,
                                              Controller::MEMORY_CONTROLLER |
                                                  Controller::CPU_CONTROLLER |
                                                  Controller::BLOCK_CONTROLLER,
                                              NO_CONTROLLER, false);
}

inline bool CgroupLimits::set_memory_soft_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(CgroupConstant::Controller::MEMORY_CONTROLLER,
                        CgroupConstant::ControllerFile::MEMORY_SOFT_LIMIT_BYTES,
                        mem_bytes);
}

inline bool CgroupLimits::set_memory_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(CgroupConstant::Controller::MEMORY_CONTROLLER,
                        CgroupConstant::ControllerFile::MEMORY_LIMIT_BYTES,
                        mem_bytes);
}

inline bool CgroupLimits::set_cpu_shares(uint64_t shares) {
  return set_controller_value_(CgroupConstant::Controller::CPU_CONTROLLER,
                        CgroupConstant::ControllerFile::CPU_SHARES, shares);
}

inline bool CgroupLimits::set_blockio_weight(uint64_t weight) {
  return set_controller_value_(CgroupConstant::Controller::BLOCK_CONTROLLER,
                        CgroupConstant::ControllerFile::BLOCKIO_WEIGHT, weight);
}

bool CgroupLimits::set_controller_value_(
    CgroupConstant::Controller controller,
    CgroupConstant::ControllerFile controller_file, uint64_t value) {
  if (!m_cgroup.isValid() ||
      !CgroupManager::getInstance().isMounted(controller)) {
    fmt::print(stderr, "Unable to set {} because cgroup {} is invalid.\n",
               CgroupConstant::GetControllerFileStringView(controller_file),
               CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  int err;
  struct cgroup *cg = &m_cgroup.getCgroup();
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
