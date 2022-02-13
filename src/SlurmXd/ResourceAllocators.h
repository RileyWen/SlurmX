#pragma once

#include "cgroup.linux.h"
#include "slurmx/PublicHeader.h"

namespace Xd {

class AllocatableResourceAllocator {
 public:
  static bool Allocate(const AllocatableResource& resource, util::Cgroup* cg);
  static bool Allocate(const SlurmxGrpc::AllocatableResource& resource,
                       util::Cgroup* cg);
};

}  // namespace Xd
