#include <fmt/format.h>
#include <libcgroup.h>
#include <sys/wait.h>
#include <unistd.h>

#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

#include "cgroup.linux.h"
#include "cgroup_limits.h"

// NOTICE: For non-RHEL system, swap account in cgroup is disabled by default.
//  Turn it on by adding
//  `GRUB_CMDLINE_LINUX="cgroup_enable=memory swapaccount=1`
//  to `/etc/default/grub` file.

// NOTICE: By default, Linux follows an optimistic memory allocation
//  strategy.  This means that when malloc() returns non-NULL there
//  is no guarantee that the memory really is available.
//  see hps://man7.org/linux/man-pages/man3/malloc.3.html
//
//  The process exceeding the memory+swap limit will be killed by oom-kill
//  (Receiving SIGTERM and exiting with return value 9).
//
//  To make malloc return NULL, see the description of
//  /proc/sys/vm/overcommit_memory and /proc/sys/vm/oom_adj in proc(5), and the
//  kernel source file Documentation/vm/overcommit-accounting

constexpr size_t B = 1;
constexpr size_t KB = 1024 * B;
constexpr size_t MB = 1024 * KB;

int child_func_cpu() {
  sleep(2);

  auto cpu_burning = [] {
    clock_t timeStart;
    timeStart = clock();
    volatile int i1, i2, i3, i4, i5, i6, i7, i8;
    i1 = i2 = i3 = i4 = i5 = i6 = i7 = i8 = 0;
    while (true) {
      i1++;
      i2++;
      i3++;
      i4++;
      i5++;
      i6++;
      i7++;
      i8++;

      if ((clock() - timeStart) / CLOCKS_PER_SEC >= 60)  // time in seconds
        break;
    }
  };

  std::thread t1(cpu_burning);
  std::thread t2(cpu_burning);
  std::thread t3(cpu_burning);

  t1.join();
  t2.join();
  t3.join();

  return 0;
}

int child_func_mem() {
  sleep(2);

  uint8_t* space = nullptr;
  try {
    size_t size = 20 * MB;
    space = new uint8_t[size];

    std::memset(space, 0, size);
  } catch (std::bad_alloc& e) {
    fmt::print("[Child] std::bad_alloc {}\n", e.what());
    space = nullptr;
  }

  (space);

  if (space) {
    delete[] space;
  }

  sleep(10);

  return 0;
}

int parent_func(pid_t child_pid) {
  fmt::print("[Parent] Child pid: {}\n", child_pid);

  const std::string cg_path{"riley_cgroup"};

  CgroupManager& cm = CgroupManager::getInstance();
  cm.create_or_open(cg_path, ALL_CONTROLLER_FLAG, NO_CONTROLLER_FLAG, false);

  CgroupLimits cg_limit(cg_path);
  // cg_limit.set_memory_limit_bytes(10 * MB);
  // cg_limit.set_memory_sw_limit_bytes(10 * MB);

  // cg_limit.set_memory_soft_limit_bytes(10 * MB);

  cg_limit.set_cpu_core_limit(2);

  cm.migrate_proc_to_cgroup(child_pid, cg_path);

  int child_ret_val;
  wait(&child_ret_val);

  fmt::print("[Parent] Child exited with {}.\n", child_ret_val);

  cm.destroy(cg_path);

  return 0;
}

int main() {
  pid_t child_pid = fork();
  if (child_pid == 0) {
    return child_func_cpu();
  } else {
    return parent_func(child_pid);
  }
}
