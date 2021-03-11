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
  SlurmxErr Parse(int argc, char **argv);


  cxxopts::ParseResult GetResult(int argc, char **argv);

  uint64_t MemoryParseClient(std::string str,
                               const cxxopts::ParseResult &result);

  TaskInfo GetTaskInfo(const cxxopts::ParseResult &result, uuid resource_uuid);

  AllocatableResource GetAllocatableResource(
      const cxxopts::ParseResult &result);

  void PrintTaskInfo(const TaskInfo task,
                     const AllocatableResource allocatableResource);
};