#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cxxopts.hpp>
#include <regex>
#include <vector>

#include "PublicHeader.h"

using boost::uuids::uuid;

class OptParse {
 public:
  struct AllocatableResource {
    uint64_t cpu_core_limit;
    uint64_t memory_limit_bytes;
    uint64_t memory_sw_limit_bytes;
  };

  struct TaskInfo {
    std::string executive_path;
    std::vector<std::string> arguments;
    uuid resource_uuid;
  };

  SlurmxErr err;
  AllocatableResource allocatableResource;
  TaskInfo taskinfo;

  cxxopts::ParseResult GetResult(int argc, char **argv);

  SlurmxErr MemoryParseClient(std::string str,
                              const cxxopts::ParseResult &result,
                              uint64_t &nmemory_byte);

  void GetTaskInfo(TaskInfo &task);
  void GetAllocatableResource(AllocatableResource &allocatableResource);
  void AddUuid(uuid uuid);
  SlurmxErr Parse(int argc, char **argv);
  void PrintTaskInfo();
};
