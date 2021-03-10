#include "opt_parse.h"

cxxopts::ParseResult opt_parse::parse(int argc, char **argv) {
  try {
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
        "t,task", "task",
        cxxopts::value<std::string>()->default_value("notask"))("help",
                                                                "Print help")(
        "positional",
        "Positional arguments: these are the arguments that are entered "
        "without an option",
        cxxopts::value<std::vector<std::string>>()->default_value(" "));

    options.parse_positional({"task", "positional"});

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
      SLURMX_INFO("\n{}", options.help({"", "Group"}));
      exit(0);
    }
    return result;
  } catch (const cxxopts::OptionException &e) {
    SLURMX_ERROR("error parsing options: {}", e.what());
    exit(1);
  }
}
uint64_t opt_parse::memory_parse_client(std::string str,
                                        const cxxopts::ParseResult &result) {
  auto nmemory = result[str].as<std::string>();
  std::regex Rnmemory("^[0-9]+[mMgG]?$");
  if (!std::regex_match(nmemory, Rnmemory)) {
    SLURMX_ERROR(
        "Error! {} must be uint number or the uint number ends with "
        "'m/M/g/G'!",
        str);

    exit(1);

  } else {
    uint64_t nmemory_byte;
    if (nmemory[nmemory.length() - 1] == 'M' ||
        nmemory[nmemory.length() - 1] == 'm') {
      std::regex Rnmemory_M("^[0-9]{1,8}[mMgG]?$");
      if (!std::regex_match(nmemory, Rnmemory_M)) {
        SLURMX_ERROR("Error! {} out of the range!", str);
        exit(1);
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length() - 1)) * 1024 *
          1024;
    } else if (nmemory[nmemory.length() - 1] == 'G' ||
               nmemory[nmemory.length() - 1] == 'g') {
      std::regex Rnmemory_G("^[0-9]{1,5}[mMgG]?$");
      if (!std::regex_match(nmemory, Rnmemory_G)) {
        SLURMX_ERROR("Error! {} out of the range!", str);
        exit(1);
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length() - 1)) * 1024 *
          1024 * 1024;
    } else {
      std::regex Rnmemory_G("^[0-9]{1,15}$");
      if (!std::regex_match(nmemory, Rnmemory_G)) {
        SLURMX_ERROR("Error! {} out of the range!", str);
        exit(1);
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length())) * 1024;
      if (nmemory_byte == 0) {
        SLURMX_ERROR("Error! {} can not be zero!", str);
        exit(1);
      }
    }
    if (nmemory_byte == 0) {
      SLURMX_ERROR("Error! {} can not be zero!", str);
      exit(1);
    }
    return nmemory_byte;
  }
}
opt_parse::TaskInfo opt_parse::GetTaskInfo(const cxxopts::ParseResult &result,
                                           uuid resource_uuid) {
  TaskInfo task;

  std::regex rexecutive_path("^\\w*$");
  std::string str = result["task"].as<std::string>();
  if (std::regex_match(str, rexecutive_path)) {
    task.executive_path = str;
  } else {
    SLURMX_ERROR(
        "Task name can only contain letters, numbers, and underscores!");
    exit(1);
  }
  for (auto arg : result["positional"].as<std::vector<std::string>>()) {
    task.arguments.push_back(arg);
  }
  task.resource_uuid = resource_uuid;
  return task;
}
opt_parse::AllocatableResource opt_parse::GetAllocatableResource(
    const cxxopts::ParseResult &result) {
  AllocatableResource allocatableResource;

  uint64_t parameter_bytes;

  parameter_bytes = result["ncpu"].as<uint64_t>();
  if (parameter_bytes == 0) {
    SLURMX_ERROR("Error! Cpu core can not be zero!");
    exit(1);
  } else {
    allocatableResource.cpu_core_limit = parameter_bytes;
  }

  parameter_bytes = memory_parse_client("nmemory", result);
  allocatableResource.memory_limit_bytes = parameter_bytes;

  parameter_bytes = memory_parse_client("nmemory_swap", result);
  allocatableResource.memory_sw_limit_bytes = parameter_bytes;

  return allocatableResource;
}
void opt_parse::PrintTaskInfo(
    const opt_parse::TaskInfo task,
    const opt_parse::AllocatableResource allocatableResource) {
  std::string args;

  for (auto arg : task.arguments) {
    args.append(arg).append(", ");
  }

  SLURMX_INFO(
      "executive_path: {}\nargments: {}\nAllocatableResource:\n cpu_byte: "
      "{}\n "
      "memory_byte: {}\n memory_sw_byte: {}\n ",
      task.executive_path, args, allocatableResource.cpu_core_limit,
      allocatableResource.memory_limit_bytes,
      allocatableResource.memory_sw_limit_bytes);
}
