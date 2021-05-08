
/*
 * Utility library for libcgroup initialization routines.
 *
 */

#include "cgroup.linux.h"

namespace Cgroup {

CgroupManager *CgroupManager::m_singleton_ = nullptr;

/*
 * Create a CgroupManager.  Note this is private - users of the CgroupManager
 * may create an instance via CgroupManager::getInstance()
 */

CgroupManager::CgroupManager() : m_mounted_controllers_() { initialize(); }

CgroupManager &CgroupManager::getInstance() {
  MutexGuard guard = CgroupManager::getGuard();

  if (m_singleton_ == nullptr) {
    m_singleton_ = new CgroupManager;
  }
  return *m_singleton_;
}

/*
 * Initialize libcgroup and mount the controllers Condor will use (if possible)
 *
 * Returns 0 on success, -1 otherwise.
 */
int CgroupManager::initialize() {
  // Initialize library and data structures
  SLURMX_DEBUG("Initializing cgroup library.");
  cgroup_init();

  // cgroup_set_loglevel(CGROUP_LOG_DEBUG);

  void *handle = nullptr;
  controller_data info{};

  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  ControllerFlags NO_CONTROLLERS;

  int ret = cgroup_get_all_controller_begin(&handle, &info);
  while (ret == 0) {
    if (info.name == GetControllerStringView(Controller::MEMORY_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::MEMORY_CONTROLLER}
                                : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::CPUACCT_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0)
          ? ControllerFlags{Controller::CPUACCT_CONTROLLER}
          : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::FREEZE_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::FREEZE_CONTROLLER}
                                : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::BLOCK_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::BLOCK_CONTROLLER}
                                : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::CPU_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::CPU_CONTROLLER}
                                : NO_CONTROLLERS;
    } else if (info.name ==
               GetControllerStringView(Controller::DEVICES_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::DEVICES_CONTROLLER}
                                : NO_CONTROLLERS;
    }
    ret = cgroup_get_all_controller_next(&handle, &info);
  }
  if (handle) {
    cgroup_get_all_controller_end(&handle);
  }

  if (!isMounted(Controller::BLOCK_CONTROLLER)) {
    SLURMX_WARN("Cgroup controller for I/O statistics is not available.\n");
  }
  if (!isMounted(Controller::FREEZE_CONTROLLER)) {
    SLURMX_WARN("Cgroup controller for process management is not available.\n");
  }
  if (!isMounted(Controller::CPUACCT_CONTROLLER)) {
    SLURMX_WARN("Cgroup controller for CPU accounting is not available.\n");
  }
  if (!isMounted(Controller::MEMORY_CONTROLLER)) {
    SLURMX_WARN("Cgroup controller for memory accounting is not available.\n");
  }
  if (!isMounted(Controller::CPU_CONTROLLER)) {
    SLURMX_WARN("Cgroup controller for CPU is not available.\n");
  }
  if (!isMounted(Controller::DEVICES_CONTROLLER)) {
    SLURMX_WARN("Cgroup controller for DEVICES is not available.\n");
  }
  if (ret != ECGEOF) {
    SLURMX_WARN("Error iterating through cgroups mount information: {}\n",
                cgroup_strerror(ret));
    return -1;
  }

  return 0;
}

/*
 * Initialize a controller for a given cgroup.
 *
 * Not designed for external users - extracted from CgroupManager::create to
 * reduce code duplication.
 */
int CgroupManager::initialize_controller(
    struct cgroup &cgroup, const CgroupConstant::Controller controller,
    const bool required, const bool has_cgroup, bool &changed_cgroup) const {
  std::string_view controller_str =
      CgroupConstant::GetControllerStringView(controller);

  if (!isMounted(controller)) {
    if (required) {
      SLURMX_WARN("Error - cgroup controller {} not mounted, but required.\n",
                  CgroupConstant::GetControllerStringView(controller));
      return 1;
    } else {
      fmt::print("cgroup controller {} is already mounted");
      return 0;
    }
  }

  if (!has_cgroup ||
      (cgroup_get_controller(&cgroup, controller_str.data()) == nullptr)) {
    changed_cgroup = true;
    if (cgroup_add_controller(&cgroup, controller_str.data()) == nullptr) {
      SLURMX_WARN("Unable to initialize cgroup {} controller.\n",
                  controller_str);
      return required ? 1 : 0;
    }
  }

  return 0;
}

/*
 * Create a new cgroup.
 * Parameters:
 *   - cgroup: reference to a Cgroup object to create/initialize.
 *   - preferred_controllers: Bitset of the controllers we would prefer.
 *   - required_controllers: Bitset of the controllers which are required.
 * Return values:
 *   - 0 on success if the cgroup is pre-existing.
 *   - -1 on error
 * On failure, the state of cgroup is undefined.
 */
bool CgroupManager::create_or_open(const std::string &cgroup_string,
                                   ControllerFlags preferred_controllers,
                                   ControllerFlags required_controllers,
                                   bool retrieve) {
  // Todo: In our design, the CgroupManager is the only owner and manager of
  //  all Cgroup in the system. Therefore, when creating a cgroup, there's no
  //  need to use the cgroup_get_cgroup in libcgroup function to check the
  //  existence of the cgroup.

  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  MutexGuard guard = getGuard();

  auto iter = m_cgroup_info_.find(cgroup_string);
  if (iter != m_cgroup_info_.end()) {
    iter->second.ref_cnt++;
    return true;
  }

  bool created_cgroup = false, changed_cgroup = false;
  struct cgroup *cgroupp = cgroup_new_cgroup(cgroup_string.c_str());
  if (cgroupp == NULL) {
    SLURMX_WARN("Unable to construct new cgroup object.\n");
    return false;
  }

  // Make sure all required controllers are in preferred controllers:
  preferred_controllers |= required_controllers;

  // Try to fill in the struct cgroup from /proc, if it exists.
  bool has_cgroup = retrieve;
  if (retrieve && (ECGROUPNOTEXIST == cgroup_get_cgroup(cgroupp))) {
    has_cgroup = false;
  }

  // Work through the various controllers.
  if ((preferred_controllers & Controller::CPUACCT_CONTROLLER) &&
      initialize_controller(
          *cgroupp, Controller::CPUACCT_CONTROLLER,
          required_controllers & Controller::CPUACCT_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return false;
  }
  if ((preferred_controllers & Controller::MEMORY_CONTROLLER) &&
      initialize_controller(
          *cgroupp, Controller::MEMORY_CONTROLLER,
          required_controllers & Controller::MEMORY_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return false;
  }
  if ((preferred_controllers & Controller::FREEZE_CONTROLLER) &&
      initialize_controller(
          *cgroupp, Controller::FREEZE_CONTROLLER,
          required_controllers & Controller::FREEZE_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return false;
  }
  if ((preferred_controllers & Controller::BLOCK_CONTROLLER) &&
      initialize_controller(*cgroupp, Controller::BLOCK_CONTROLLER,
                            required_controllers & Controller::BLOCK_CONTROLLER,
                            has_cgroup, changed_cgroup)) {
    return false;
  }
  if ((preferred_controllers & Controller::CPU_CONTROLLER) &&
      initialize_controller(*cgroupp, Controller::CPU_CONTROLLER,
                            required_controllers & Controller::CPU_CONTROLLER,
                            has_cgroup, changed_cgroup)) {
    return false;
  }
  if ((preferred_controllers & Controller::DEVICES_CONTROLLER) &&
      initialize_controller(
          *cgroupp, Controller::DEVICES_CONTROLLER,
          required_controllers & Controller::DEVICES_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return false;
  }



  int err;
  if (!has_cgroup) {
    if ((err = cgroup_create_cgroup(cgroupp, 0))) {
      // Only record at D_ALWAYS if any cgroup mounts are available.
      SLURMX_WARN(
          "Unable to create cgroup {}."
          " Cgroup functionality will not work: {}\n",
          cgroup_string.c_str(), cgroup_strerror(err));
      return false;
    } else {
      created_cgroup = true;
    }
  } else if (has_cgroup && changed_cgroup &&
             (err = cgroup_modify_cgroup(cgroupp))) {
    SLURMX_WARN(
        "Unable to modify cgroup {}."
        "  Some cgroup functionality may not work: {} {}\n",
        cgroup_string.c_str(), err, cgroup_strerror(err));
  }

  // Try to turn on hierarchical memory accounting.
  struct cgroup_controller *mem_controller = cgroup_get_controller(
      cgroupp, GetControllerStringView(Controller::MEMORY_CONTROLLER).data());
  if (retrieve && isMounted(Controller::MEMORY_CONTROLLER) && created_cgroup &&
      (mem_controller != NULL)) {
    if ((err = cgroup_add_value_bool(mem_controller, "memory.use_hierarchy",
                                     true))) {
      SLURMX_WARN("Unable to set hierarchical memory settings for {}: {} {}\n",
                  cgroup_string.c_str(), err, cgroup_strerror(err));
    } else {
      if ((err = cgroup_modify_cgroup(cgroupp))) {
        SLURMX_WARN(
            "Unable to enable hierarchical memory accounting for {} "
            ": {} {}\n",
            cgroup_string.c_str(), err, cgroup_strerror(err));
      }
    }
  }

  CgroupInfo cg_info;
  cg_info.cgroup_ptr = std::make_unique<Internal::Cgroup>();

  // Finally, fill in the Cgroup object's state:
  cg_info.cgroup_ptr->setCgroupString(cgroup_string);
  cg_info.cgroup_ptr->setCgroup(*cgroupp);
  cg_info.ref_cnt = 1;

  m_cgroup_info_[cgroup_string] = std::move(cg_info);

  return true;
}

/*
 * Delete the cgroup in the OS.
 * Returns true on success, false on failure;
 */
bool CgroupManager::destroy(const std::string &cgroup_path) {
  MutexGuard guard = getGuard();

  auto it = m_cgroup_info_.find(cgroup_path);
  if (it == m_cgroup_info_.end()) {
    SLURMX_WARN("Destroying an unknown cgroup.");
  }
  it->second.ref_cnt--;

  // Only delete if this is the last ref and we originally created it.
  if (it->second.ref_cnt == 0) {
    int err;
    // Must re-initialize the cgroup structure before deletion.
    struct cgroup *dcg = cgroup_new_cgroup(cgroup_path.c_str());
    assert(dcg != nullptr);
    if ((err = cgroup_get_cgroup(dcg))) {
      SLURMX_WARN("Unable to read cgroup {} for deletion: {} {}\n",
                  cgroup_path.c_str(), err, cgroup_strerror(err));
      cgroup_free(&dcg);
      return false;
    }

    // CGFLAG_DELETE_EMPTY_ONLY is set to avoid libgroup from finding parent
    // cgroup, which is usually the mount point of root cgroup and will cause
    // ENOENT error.
    //
    // Todo: Test this part when cgroup is not empty!
    if ((err = cgroup_delete_cgroup_ext(
        dcg, CGFLAG_DELETE_EMPTY_ONLY | CGFLAG_DELETE_IGNORE_MIGRATION))) {
      SLURMX_WARN("Unable to completely remove cgroup {}: {} {}\n",
                  cgroup_path.c_str(), err, cgroup_strerror(err));
    } else {
      SLURMX_WARN("Deleted cgroup {}.", cgroup_path.c_str());
    }

    // Notice the cgroup struct freed here is not the one held by Cgroup class.
    cgroup_free(&dcg);

    // This call results in the destructor call of Cgroup, which frees the
    // internal libcgroup struct.
    m_cgroup_info_.erase(cgroup_path);
  }

  return true;
}

bool CgroupManager::migrate_proc_to_cgroup(pid_t pid,
                                           const std::string &cgroup_path) {
  // Attempt to migrate a given process to a cgroup.
  // This can be done without regards to whether the
  // process is already in the cgroup
  auto iter = m_cgroup_info_.find(cgroup_path);
  if (iter == m_cgroup_info_.end()) {
    SLURMX_WARN(cgroup_path);
    return false;
  }

  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  // We want to make sure task migration is turned on for the
  // associated memory controller.  So, we get to look up the original cgroup.
  //
  // If there is no memory controller present, we skip all this and just attempt
  // a migrate
  int err;
  u_int64_t orig_migrate;
  bool changed_orig = false;
  char *orig_cgroup_path = nullptr;
  struct cgroup *orig_cgroup;
  struct cgroup_controller *memory_controller;
  if (isMounted(Controller::MEMORY_CONTROLLER) &&
      (err = cgroup_get_current_controller_path(
          pid, GetControllerStringView(Controller::MEMORY_CONTROLLER).data(),
          &orig_cgroup_path))) {
    SLURMX_WARN(
        "Unable to determine current memory cgroup for PID {}. Error {}: {}\n",
        pid, err, cgroup_strerror(err));
    return false;
  }
  // We will migrate the PID to the new cgroup even if it is in the proper
  // memory controller cgroup It is possible for the task to be in multiple
  // cgroups.
  if (isMounted(Controller::MEMORY_CONTROLLER) && (orig_cgroup_path != NULL) &&
      (cgroup_path == orig_cgroup_path)) {
    // Yes, there are race conditions here - can't really avoid this.
    // Throughout this block, we can assume memory controller exists.
    // Get original value of migrate.
    orig_cgroup = cgroup_new_cgroup(orig_cgroup_path);
    assert(orig_cgroup != nullptr);
    if ((err = cgroup_get_cgroup(orig_cgroup))) {
      SLURMX_WARN("Unable to read original cgroup {}. Error {}: {}\n",
                  orig_cgroup_path, err, cgroup_strerror(err));
      cgroup_free(&orig_cgroup);
      goto after_migrate;
    }
    if ((memory_controller = cgroup_get_controller(
        orig_cgroup,
        GetControllerStringView(Controller::MEMORY_CONTROLLER).data())) ==
        nullptr) {
      SLURMX_WARN(
          "Unable to get memory controller of cgroup {}. Error {}: {}\n",
          orig_cgroup_path, err, cgroup_strerror(err));
      cgroup_free(&orig_cgroup);
      goto after_migrate;
    }
    if ((err = cgroup_get_value_uint64(memory_controller,
                                       "memory.move_charge_at_immigrate",
                                       &orig_migrate))) {
      if (err == ECGROUPVALUENOTEXIST) {
        // Older kernels don't have the ability to migrate memory accounting
        // to the new cgroup.
        SLURMX_WARN(
            "This kernel does not support memory usage migration; cgroup "
            "{} memory statistics"
            " will be slightly incorrect.\n",
            cgroup_path.c_str());
      } else {
        SLURMX_WARN(
            "Unable to read cgroup {} memory controller settings for "
            "migration: {} {}\n",
            orig_cgroup_path, err, cgroup_strerror(err));
      }
      cgroup_free(&orig_cgroup);
      goto after_migrate;
    }
    if (orig_migrate != 3) {
      cgroup_free(&orig_cgroup);
      orig_cgroup = cgroup_new_cgroup(orig_cgroup_path);
      memory_controller = cgroup_add_controller(
          orig_cgroup,
          GetControllerStringView(Controller::MEMORY_CONTROLLER).data());
      assert(memory_controller !=
             NULL);  // Memory controller must already exist
      cgroup_add_value_uint64(memory_controller,
                              "memory.move_charge_at_immigrate", 3);
      if ((err = cgroup_modify_cgroup(orig_cgroup))) {
        // Not allowed to change settings
        SLURMX_WARN(
            "Unable to change cgroup {} memory controller settings for "
            "migration. "
            "Some memory accounting will be inaccurate: {} "
            "{}\n",
            orig_cgroup_path, err, cgroup_strerror(err));
      } else {
        changed_orig = true;
      }
    }
    cgroup_free(&orig_cgroup);
  }

  after_migrate:

  orig_cgroup = NULL;
  err = cgroup_attach_task_pid(
      &const_cast<struct cgroup &>(iter->second.cgroup_ptr->getCgroup()), pid);
  if (err) {
    SLURMX_WARN("Cannot attach pid {} to cgroup {}: {} {}\n", pid,
                cgroup_path.c_str(), err, cgroup_strerror(err));
  }

  if (changed_orig) {
    if ((orig_cgroup = cgroup_new_cgroup(orig_cgroup_path)) == NULL) {
      goto after_restore;
    }
    if (((memory_controller = cgroup_add_controller(
        orig_cgroup,
        GetControllerStringView(Controller::MEMORY_CONTROLLER).data())) !=
         nullptr) &&
        (!cgroup_add_value_uint64(memory_controller,
                                  "memory.move_charge_at_immigrate",
                                  orig_migrate))) {
      if ((err = cgroup_modify_cgroup(orig_cgroup))) {
        SLURMX_WARN(
            "Unable to change cgroup {} memory controller settings for "
            "migration. "
            "Some memory accounting will be inaccurate: {} "
            "{}\n",
            orig_cgroup_path, err, cgroup_strerror(err));
      } else {
        changed_orig = true;
      }
    }
    cgroup_free(&orig_cgroup);
  }

  after_restore:
  if (orig_cgroup_path != nullptr) {
    free(orig_cgroup_path);
  }
  return err == 0 ? true : false;
}

std::optional<CgroupManager::CgroupInfoCRefWrapper> CgroupManager::find_cgroup(
    const std::string &cgroup_path) {
  auto iter = m_cgroup_info_.find(cgroup_path);
  if (iter == m_cgroup_info_.end()) return std::nullopt;

  return iter->second;
}

bool CgroupManager::set_cgroup_limit(const Internal::Cgroup &cg,
                                     const CgroupLimit &cg_limit) {
  bool ret = true;
  Internal::CgroupManipulator cg_manipulator(cg);

  if (cg_limit.cpu_core_limit != 0)
    ret &= cg_manipulator.set_cpu_core_limit(cg_limit.cpu_core_limit);

  if (cg_limit.cpu_shares != 0)
    ret &= cg_manipulator.set_cpu_shares(cg_limit.cpu_shares);

  if (cg_limit.memory_limit_bytes != 0)
    ret &= cg_manipulator.set_memory_limit_bytes(cg_limit.memory_limit_bytes);

  if (cg_limit.memory_sw_limit_bytes != 0)
    ret &= cg_manipulator.set_memory_sw_limit_bytes(
        cg_limit.memory_sw_limit_bytes);

  if (cg_limit.memory_soft_limit_bytes != 0)
    ret &= cg_manipulator.set_memory_soft_limit_bytes(
        cg_limit.memory_soft_limit_bytes);

  if (cg_limit.blockio_weight != 0)
    ret &= cg_manipulator.set_blockio_weight(cg_limit.blockio_weight);

  return ret;
}

namespace Internal {

/*
 * Cleanup cgroup.
 * If the cgroup was created by us in the OS, remove it..
 */
Cgroup::~Cgroup() { destroy(); }

void Cgroup::setCgroup(struct cgroup &cgroup) {
  if (m_cgroup_) {
    destroy();
  }
  m_cgroup_ = &cgroup;
}

void Cgroup::destroy() {
  if (m_cgroup_) {
    cgroup_free(&m_cgroup_);
    m_cgroup_ = nullptr;
  }
}

CgroupManipulator::CgroupManipulator(const Cgroup &cg) : m_cgroup_(cg) {}

bool CgroupManipulator::set_memory_soft_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_SOFT_LIMIT_BYTES, mem_bytes);
}

bool CgroupManipulator::set_memory_sw_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_MEMSW_LIMIT_IN_BYTES, mem_bytes);
}

bool CgroupManipulator::set_memory_limit_bytes(uint64_t mem_bytes) {
  return set_controller_value_(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_LIMIT_BYTES, mem_bytes);
}

bool CgroupManipulator::set_cpu_shares(uint64_t shares) {
  return set_controller_value_(CgroupConstant::Controller::CPU_CONTROLLER,
                               CgroupConstant::ControllerFile::CPU_SHARES,
                               shares);
}
bool CgroupManipulator::set_cpu_core_limit(uint64_t core_num) {
  constexpr uint32_t base = 1000'000;

  bool ret;
  ret = set_controller_value_(CgroupConstant::Controller::CPU_CONTROLLER,
                              CgroupConstant::ControllerFile::CPU_CFS_QUOTA_US,
                              base * core_num);
  ret &= set_controller_value_(
      CgroupConstant::Controller::CPU_CONTROLLER,
      CgroupConstant::ControllerFile::CPU_CFS_PERIOD_US, base);

  return ret;
}

bool CgroupManipulator::set_blockio_weight(uint64_t weight) {
  return set_controller_value_(CgroupConstant::Controller::BLOCK_CONTROLLER,
                               CgroupConstant::ControllerFile::BLOCKIO_WEIGHT,
                               weight);
}

bool CgroupManipulator::set_devices_deny(std::string deny_devices){
  return set_controller_string_(CgroupConstant::Controller::DEVICES_CONTROLLER,
                                CgroupConstant::ControllerFile::DEVICES_DENY,
                                deny_devices);
}
bool CgroupManipulator::set_devices_allow(std::string allow_devices){
  return set_controller_string_(CgroupConstant::Controller::DEVICES_CONTROLLER,
                                CgroupConstant::ControllerFile::DEVICES_ALLOW,
                                allow_devices);
}

bool CgroupManipulator::set_controller_string_(
    CgroupConstant::Controller controller,
    CgroupConstant::ControllerFile controller_file, const std::string &value) {
  CgroupManager &cm = CgroupManager::getInstance();

  if (!cm.isMounted(controller)) {
    SLURMX_WARN("Unable to set {} because cgroup {} is not mounted.\n",
                CgroupConstant::GetControllerFileStringView(controller_file),
                CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  int err;

  // a bit dirty here
  auto *cg = const_cast<cgroup *>(&m_cgroup_.getCgroup());

  struct cgroup_controller *cg_controller;

  if ((cg_controller = cgroup_get_controller(
      cg, CgroupConstant::GetControllerStringView(controller).data())) ==
      nullptr) {
    SLURMX_WARN("Unable to get cgroup {} controller for {}.\n",
                CgroupConstant::GetControllerStringView(controller),
                m_cgroup_.getCgroupString());
    return false;
  }

  if ((err = cgroup_set_value_string(
      cg_controller,
      CgroupConstant::GetControllerFileStringView(controller_file).data(),
      value.c_str()))) {
    SLURMX_WARN("Unable to set block IO weight for {}: {} {}\n",
                m_cgroup_.getCgroupString(), err, cgroup_strerror(err));
    return false;
  }

  // Commit cgroup modifications.
  if ((err = cgroup_modify_cgroup(cg))) {
    SLURMX_WARN("Unable to commit {} for cgroup {}: {} {}\n",
                CgroupConstant::GetControllerFileStringView(controller_file),
                m_cgroup_.getCgroupString(), err, cgroup_strerror(err));
    return false;
  }

  return true;
}

bool CgroupManipulator::set_controller_value_(
    CgroupConstant::Controller controller,
    CgroupConstant::ControllerFile controller_file, uint64_t value) {
  CgroupManager &cm = CgroupManager::getInstance();

  if (!cm.isMounted(controller)) {
    SLURMX_WARN("Unable to set {} because cgroup {} is not mounted.\n",
                CgroupConstant::GetControllerFileStringView(controller_file),
                CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  int err;

  // a bit dirty here
  auto *cg = const_cast<cgroup *>(&m_cgroup_.getCgroup());

  struct cgroup_controller *cg_controller;

  if ((cg_controller = cgroup_get_controller(
      cg, CgroupConstant::GetControllerStringView(controller).data())) ==
      nullptr) {
    SLURMX_WARN("Unable to get cgroup {} controller for {}.\n",
                CgroupConstant::GetControllerStringView(controller),
                m_cgroup_.getCgroupString());
    return false;
  }

  if ((err = cgroup_set_value_uint64(
      cg_controller,
      CgroupConstant::GetControllerFileStringView(controller_file).data(),
      value))) {
    SLURMX_WARN("Unable to set block IO weight for {}: {} {}\n",
                m_cgroup_.getCgroupString(), err, cgroup_strerror(err));
    return false;
  }

  // Commit cgroup modifications.
  if ((err = cgroup_modify_cgroup(cg))) {
    SLURMX_WARN("Unable to commit {} for cgroup {}: {} {}\n",
                CgroupConstant::GetControllerFileStringView(controller_file),
                m_cgroup_.getCgroupString(), err, cgroup_strerror(err));
    return false;
  }

  return true;
}

}  // namespace Internal

}  // namespace Cgroup