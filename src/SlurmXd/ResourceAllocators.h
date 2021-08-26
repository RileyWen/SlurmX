#pragma once

#include "PublicHeader.h"
#include "cgroup.linux.h"

namespace Xd {

class AllocatableResourceAllocator {
 public:
  static bool Allocate(const AllocatableResource& resource, util::Cgroup* cg);
};

}  // namespace Xd
