#include "OptParse.h"

cxxopts::ParseResult OptParse::GetResult(int argc, char **argv) {
  cxxopts::Options options(argv[0], " - srunX command line options");
  options.positional_help("task_name [Task Args...]").show_positional_help();
  options.add_options()("c,ncpu", "limiting the cpu usage of task",
                        cxxopts::value<uint64_t>()->default_value("2"))(
      "s,ncpu_shares", "limiting the cpu shares of task",
      cxxopts::value<uint64_t>()->default_value("2"))(
      "m,nmemory", "limiting the memory usage of task",
      cxxopts::value<std::string>()->default_value("128M"))(
      "w,nmemory_swap", "limiting the swap memory usage of task",
      cxxopts::value<std::string>()->default_value("128M"))(
      "f,nmemory_soft", "limiting the soft memory usage of task",
      cxxopts::value<std::string>()->default_value("128M"))(
      "b,blockio_weight", "limiting the weight of blockio",
      cxxopts::value<std::string>()->default_value("128M"))(
      "t,task", "task", cxxopts::value<std::string>()->default_value("notask"))(
      "help", "Print help")(
      "positional",
      "Positional arguments: these are the arguments that are entered "
      "without an option",
      cxxopts::value<std::vector<std::string>>()->default_value(" "));

  options.parse_positional({"task", "positional"});

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    SLURMX_INFO("\n{}", options.help({"", "Group"}));
  }
  return result;
}

SlurmxErr OptParse::MemoryParseClient(std::string str,
                                      const cxxopts::ParseResult &result,
                                      uint64_t &nmemory_byte) {
  auto nmemory = result[str].as<std::string>();
  std::regex Rnmemory("^[0-9]+[mMgG]?$");
  if (!std::regex_match(nmemory, Rnmemory)) {
    SLURMX_ERROR(
        "Error! {} must be uint number or the uint number ends with "
        "'m/M/g/G'!",
        str);
    return SlurmxErr::kOptParseTypeErr;
  } else {
    if (nmemory[nmemory.length() - 1] == 'M' ||
        nmemory[nmemory.length() - 1] == 'm') {
      std::regex Rnmemory_M("^[0-9]{1,8}[mMgG]?$");
      if (!std::regex_match(nmemory, Rnmemory_M)) {
        SLURMX_ERROR("Error! {} out of the range!", str);
        return SlurmxErr::kOptParseRangeErr;
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length() - 1)) * 1024 *
          1024;
    } else if (nmemory[nmemory.length() - 1] == 'G' ||
               nmemory[nmemory.length() - 1] == 'g') {
      std::regex Rnmemory_G("^[0-9]{1,5}[mMgG]?$");
      if (!std::regex_match(nmemory, Rnmemory_G)) {
        SLURMX_ERROR("Error! {} out of the range!", str);
        return SlurmxErr::kOptParseRangeErr;
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length() - 1)) * 1024 *
          1024 * 1024;
    } else {
      std::regex Rnmemory_G("^[0-9]{1,15}$");
      if (!std::regex_match(nmemory, Rnmemory_G)) {
        SLURMX_ERROR("Error! {} out of the range!", str);
        return SlurmxErr::kOptParseRangeErr;
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length())) * 1024;
      if (nmemory_byte == 0) {
        SLURMX_ERROR("Error! {} can not be zero!", str);
        return SlurmxErr::kOptParseZeroErr;
      }
    }
    if (nmemory_byte == 0) {
      SLURMX_ERROR("Error! {} can not be zero!", str);
      return SlurmxErr::kOptParseZeroErr;
    }
    return SlurmxErr::kOk;
  }
}

void OptParse::AddUuid(uuid uuid) { taskinfo.resource_uuid = uuid; }

void OptParse::GetAllocatableResource(OptParse::AllocatableResource &alloRes) {
  alloRes.memory_limit_bytes = allocatableResource.memory_limit_bytes;
  alloRes.cpu_core_limit = allocatableResource.cpu_core_limit;
  alloRes.memory_sw_limit_bytes = allocatableResource.memory_sw_limit_bytes;
}

void OptParse::GetTaskInfo(TaskInfo &task) {
  task.executive_path = taskinfo.executive_path;
  for (auto arg : taskinfo.arguments) {
    task.arguments.push_back(arg);
  }
  task.resource_uuid = taskinfo.resource_uuid;
}

SlurmxErr OptParse::Parse(int argc, char **argv) {
  try {
    auto result = GetResult(argc, argv);

    uint64_t parameter_bytes_ncpu;
    uint64_t parameter_bytes_nmemory;
    uint64_t parameter_bytes_nmemory_swap;

    SlurmxErr err_nmemory;
    SlurmxErr err_nmemory_swap;

    parameter_bytes_ncpu = result["ncpu"].as<uint64_t>();
    if (parameter_bytes_ncpu == 0) {
      SLURMX_ERROR("Error! Cpu core can not be zero!");
      return SlurmxErr::kOptParseZeroErr;
    } else {
      allocatableResource.cpu_core_limit = parameter_bytes_ncpu;
    }

    err_nmemory = MemoryParseClient("nmemory", result, parameter_bytes_nmemory);
    allocatableResource.memory_limit_bytes = parameter_bytes_nmemory;

    err_nmemory_swap =
        MemoryParseClient("nmemory_swap", result, parameter_bytes_nmemory_swap);
    allocatableResource.memory_sw_limit_bytes = parameter_bytes_nmemory_swap;

    std::regex rexecutive_path("^\\w*$");
    std::string str = result["task"].as<std::string>();
    if (std::regex_match(str, rexecutive_path)) {
      taskinfo.executive_path = str;
    } else {
      SLURMX_ERROR(
          "Task name can only contain letters, numbers, and underscores!");
      return SlurmxErr::kOptParseTypeErr;
    }
    for (auto arg : result["positional"].as<std::vector<std::string>>()) {
      taskinfo.arguments.push_back(arg);
    }

    if (err_nmemory != SlurmxErr::kOk || err_nmemory_swap != SlurmxErr::kOk) {
      return (err_nmemory != SlurmxErr::kOk) ? err_nmemory : err_nmemory_swap;
    }

    return SlurmxErr::kOk;
  } catch (const cxxopts::OptionException &e) {
    SLURMX_ERROR("Opt Parse Failed!");
    return SlurmxErr::kOptParseFailed;
  }
}

void OptParse::PrintTaskInfo() {
  std::string args;

  for (auto arg : taskinfo.arguments) {
    args.append(arg).append(", ");
  }

  SLURMX_INFO(
      "executive_path: {}\nargments: {}\nAllocatableResource:\n cpu_byte: "
      "{}\n "
      "memory_byte: {}\n memory_sw_byte: {}\n ",
      taskinfo.executive_path, args, allocatableResource.cpu_core_limit,
      allocatableResource.memory_limit_bytes,
      allocatableResource.memory_sw_limit_bytes);
}
