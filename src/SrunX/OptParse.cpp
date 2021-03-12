#include "OptParse.h"

cxxopts::ParseResult OptParse::GetResult(int argc, char **argv,
                                         bool &help_falg) {
  cxxopts::Options options(argv[0], " - srunX command line options");
  options.positional_help("task_name [Task Args...]").show_positional_help();
  options.add_options()("s,Xdserver-address", "SlurmXd server address",
                        cxxopts::value<std::string>())(
      "p,Xdserver-port", "SlurmXd server port", cxxopts::value<std::string>())(
      "S,CtlXdserver-address", "SlurmCtlXd server address",
      cxxopts::value<std::string>())("P,CtXdserver-port",
                                     "SlurmCtlXd server port",
                                     cxxopts::value<std::string>())(
      "c,ncpu", "limiting the cpu usage of task", cxxopts::value<uint64_t>())(
      "m,nmemory", "limiting the memory usage of task",
      cxxopts::value<std::string>())("w,nmemory_swap",
                                     "limiting the swap memory usage of task",
                                     cxxopts::value<std::string>())(
      "t,task", "task", cxxopts::value<std::string>())("help", "Print help")(
      "positional",
      "Positional arguments: these are the arguments that are entered "
      "without an option",
      cxxopts::value<std::vector<std::string>>()->default_value(" "));

  //  ("s,ncpu_shares", "limiting the cpu shares of task",
  //   cxxopts::value<uint64_t>()->default_value("2"))(
  //      "f,nmemory_soft", "limiting the soft memory usage of task",
  //      cxxopts::value<std::string>()->default_value("128M"))(
  //      "b,blockio_weight", "limiting the weight of blockio",
  //      cxxopts::value<std::string>()->default_value("128M"))

  options.parse_positional({"task", "positional"});

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    SLURMX_INFO("\n{}", options.help({"", "Group"}));
    help_falg = true;
  }
  return result;
}

SlurmxErr OptParse::MemoryParseClient(std::string str,
                                      const cxxopts::ParseResult &result,
                                      uint64_t &nmemory_byte) {
  auto nmemory = result[str].as<std::string>();
  std::regex Rnmemory("^[0-9]+[mMgG]?$");
  if (!std::regex_match(nmemory, Rnmemory)) {
    SLURMX_DEBUG(
        "{} must be uint number or the uint number ends with "
        "'m/M/g/G'!",
        str);
    return SlurmxErr::kOptParseTypeErr;
  } else {
    if (nmemory[nmemory.length() - 1] == 'M' ||
        nmemory[nmemory.length() - 1] == 'm') {
      std::regex Rnmemory_M("^[0-9]{1,8}[mMgG]?$");
      if (!std::regex_match(nmemory, Rnmemory_M)) {
        SLURMX_DEBUG("{} out of the range!", str);
        return SlurmxErr::kOptParseRangeErr;
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length() - 1)) * 1024 *
          1024;
    } else if (nmemory[nmemory.length() - 1] == 'G' ||
               nmemory[nmemory.length() - 1] == 'g') {
      std::regex Rnmemory_G("^[0-9]{1,5}[mMgG]?$");
      if (!std::regex_match(nmemory, Rnmemory_G)) {
        SLURMX_DEBUG("{} out of the range!", str);
        return SlurmxErr::kOptParseRangeErr;
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length() - 1)) * 1024 *
          1024 * 1024;
    } else {
      std::regex Rnmemory_G("^[0-9]{1,15}$");
      if (!std::regex_match(nmemory, Rnmemory_G)) {
        SLURMX_DEBUG("{} out of the range!", str);
        return SlurmxErr::kOptParseRangeErr;
      }
      nmemory_byte =
          (uint64_t)std::stoi(nmemory.substr(0, nmemory.length())) * 1024;
      if (nmemory_byte == 0) {
        SLURMX_DEBUG("{} can not be zero!", str);
        return SlurmxErr::kOptParseZeroErr;
      }
    }
    if (nmemory_byte == 0) {
      SLURMX_DEBUG("{} can not be zero!", str);
      return SlurmxErr::kOptParseZeroErr;
    }
    return SlurmxErr::kOk;
  }
}

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
    bool heip_flag = false;
    auto result = GetResult(argc, argv, heip_flag);
    if (heip_flag == true) {
      return SlurmxErr::kOptHelp;
    }

    uint64_t parameter_bytes_ncpu;
    uint64_t parameter_bytes_nmemory;
    uint64_t parameter_bytes_nmemory_swap;

    SlurmxErr err_nmemory;
    SlurmxErr err_nmemory_swap;

    if (result.count("ncpu") == 0) {
      SLURMX_DEBUG("cpu must be specified.");
      return SlurmxErr::kNoCpu;
    }
    if (result.count("nmemory") == 0) {
      SLURMX_DEBUG("memory must be specified.");
      return SlurmxErr::kNoMem;
    }
    if (result.count("nmemory_swap") == 0) {
      SLURMX_DEBUG("memory_swap must be specified.");
      return SlurmxErr::kNoMemSw;
    }
    if (result.count("task") == 0) {
      SLURMX_DEBUG("task must be specified.");
      return SlurmxErr::kNoTask;
    }

    parameter_bytes_ncpu = result["ncpu"].as<uint64_t>();
    if (parameter_bytes_ncpu == 0) {
      SLURMX_DEBUG("Cpu core can not be zero!");
      return SlurmxErr::kOptParseZeroErr;
    } else {
      allocatableResource.cpu_core_limit = parameter_bytes_ncpu;
    }

    err_nmemory = MemoryParseClient("nmemory", result, parameter_bytes_nmemory);
    allocatableResource.memory_limit_bytes = parameter_bytes_nmemory;
    if (err_nmemory != SlurmxErr::kOk) {
      return err_nmemory;
    }
    err_nmemory_swap =
        MemoryParseClient("nmemory_swap", result, parameter_bytes_nmemory_swap);
    allocatableResource.memory_sw_limit_bytes = parameter_bytes_nmemory_swap;
    if (err_nmemory_swap != SlurmxErr::kOk) {
      return err_nmemory_swap;
    }
    std::regex rexecutive_path("^\\w*$");
    std::string str = result["task"].as<std::string>();
    if (std::regex_match(str, rexecutive_path)) {
      taskinfo.executive_path = str;
    } else {
      SLURMX_DEBUG(
          "Task name can only contain letters, numbers, and underscores!");
      return SlurmxErr::kOptParseTypeErr;
    }
    for (auto arg : result["positional"].as<std::vector<std::string>>()) {
      taskinfo.arguments.push_back(arg);
    }

    if (result.count("Xdserver-address") == 0) {
      SLURMX_DEBUG("SlurmCtlXd address must be specified.");
      return SlurmxErr::kNoAddress;
    }

    if (result.count("CtlXdserver-address") == 0) {
      SLURMX_DEBUG("SlurmCtlXd address must be specified.");
      return SlurmxErr::kNoAddress;
    }
    if (result.count("Xdserver-port") == 0) {
      SLURMX_DEBUG("SlurmXd port must be specified.");
      return SlurmxErr::kNoPort;
    }
    if (result.count("CtXdserver-port") == 0) {
      SLURMX_DEBUG("SlurmXd port must be specified.");
      return SlurmxErr::kNoPort;
    }

    std::string Xdserver_address = result["Xdserver-address"].as<std::string>();
    std::string Xdserver_port = result["Xdserver-port"].as<std::string>();
    std::string CtlXdserver_address =
        result["CtlXdserver-address"].as<std::string>();
    std::string CtXdserver_port = result["CtXdserver-port"].as<std::string>();

    std::regex regex_addr(
        R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

    std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                          R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

    if (!std::regex_match(Xdserver_address, regex_addr)) {
      SLURMX_DEBUG("Xdserver address is invalid.");
      return SlurmxErr::kAddressInvalid;
    }

    if (!std::regex_match(Xdserver_port, regex_port)) {
      SLURMX_DEBUG("Xdserver port is invalid.");
      return SlurmxErr::kPortInvalid;
    }

    if (!std::regex_match(CtlXdserver_address, regex_addr)) {
      SLURMX_DEBUG("CtXdserver address is invalid.");
      return SlurmxErr::kAddressInvalid;
    }

    if (!std::regex_match(CtXdserver_port, regex_port)) {
      SLURMX_DEBUG("Xdserver port is invalid.");
      return SlurmxErr::kPortInvalid;
    }
    Xdserver_addr_port = fmt::format("{}:{}", Xdserver_address, Xdserver_port);
    CtlXdserver_addr_port =
        fmt::format("{}:{}", CtlXdserver_address, CtXdserver_port);

    return SlurmxErr::kOk;
  } catch (const cxxopts::OptionException &e) {
    SLURMX_DEBUG("Opt Parse Failed!");
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
