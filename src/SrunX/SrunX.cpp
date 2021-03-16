#include "PublicHeader.h"
#include "SrunXClient.h"

std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
    SrunXClient::m_stream_;
int main(int argc, char **argv) {

  std::thread shutdown([]() {
    sleep(3);
    kill(getpid(), SIGINT);
  });
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif
  SrunXClient client;

  cxxopts::Options options(argv[0], " - srunX command line options");
  options.positional_help("task_name [Task Args...]").show_positional_help();
  options.add_options()("s,Xdserver-address", "SlurmXd server address",
                        cxxopts::value<std::string>())(
      "p,Xdserver-port", "SlurmXd server port", cxxopts::value<std::string>())(
      "S,CtlXdserver-address", "SlurmCtlXd server address",
      cxxopts::value<std::string>())("P,CtXdserver-port",
                                     "SlurmCtlXd server port",
                                     cxxopts::value<std::string>())(
      "c,cpu", "limiting the cpu usage of task", cxxopts::value<uint64_t>())(
      "m,memory", "limiting the memory usage of task",
      cxxopts::value<std::string>())("w,memory_swap",
                                     "limiting the swap memory usage of task",
                                     cxxopts::value<std::string>())(
      "t,task_executive_path", "task executive_path", cxxopts::value<std::string>())("help", "Print help")(
      "positional",
      "Positional arguments: these are the arguments that are entered "
      "without an option",
      cxxopts::value<std::vector<std::string>>()->default_value(" "));

  options.parse_positional({"task_executive_path", "positional"});
  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    SLURMX_INFO("\n{}", options.help());
    return 0;
  }
  uint64_t parameter_bytes_cpu;
  uint64_t parameter_bytes_memory;
  uint64_t parameter_bytes_memory_swap;

  if (result.count("cpu") == 0) {
    SLURMX_DEBUG("cpu must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("memory") == 0) {
    SLURMX_DEBUG("memory must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("memory_swap") == 0) {
    SLURMX_DEBUG("memory_swap must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("task_executive_path") == 0) {
    SLURMX_DEBUG("task executive path must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("Xdserver-address") == 0) {
    SLURMX_DEBUG("SlurmCtlXd address must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("CtlXdserver-address") == 0) {
    SLURMX_DEBUG("SlurmCtlXd address must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("Xdserver-port") == 0) {
    SLURMX_DEBUG("SlurmXd port must be specified.\n{}", options.help());
    return 1;
  }
  if (result.count("CtXdserver-port") == 0) {
    SLURMX_DEBUG("SlurmXd port must be specified.\n{}", options.help());
    return 1;
  }

  parameter_bytes_cpu = result["cpu"].as<uint64_t>();
  if (parameter_bytes_cpu == 0) {
    SLURMX_DEBUG("Cpu core can not be zero!\n{}", options.help());
    return 1;
  } else {
    client.allocatableResource.cpu_core_limit = parameter_bytes_cpu;
  }

  auto MemoryParseClient =
      [&](std::string str, const cxxopts::ParseResult &result,
          uint64_t &memory_byte, cxxopts::Options options) -> int {
    auto memory = result[str].as<std::string>();
    std::regex Rmemory("^[0-9]+[mMgG]?$");
    if (!std::regex_match(memory, Rmemory)) {
      SLURMX_DEBUG(
          "{} must be uint number or the uint number ends with "
          "'m/M/g/G'!\n{}",
          str, options.help());
      return 1;
    } else {
      if (memory[memory.length() - 1] == 'M' ||
          memory[memory.length() - 1] == 'm') {
        std::regex Rmemory_M("^[0-9]{1,8}[mMgG]?$");
        if (!std::regex_match(memory, Rmemory_M)) {
          SLURMX_DEBUG("{} out of the range!\n{}", str, options.help());
          return 1;
        }
        memory_byte =
            (uint64_t)std::stoi(memory.substr(0, memory.length() - 1)) * 1024 *
            1024;
      } else if (memory[memory.length() - 1] == 'G' ||
                 memory[memory.length() - 1] == 'g') {
        std::regex Rmemory_G("^[0-9]{1,5}[mMgG]?$");
        if (!std::regex_match(memory, Rmemory_G)) {
          SLURMX_DEBUG("{} out of the range!\n{}", str, options.help());
          return 1;
        }
        memory_byte =
            (uint64_t)std::stoi(memory.substr(0, memory.length() - 1)) * 1024 *
            1024 * 1024;
      } else {
        std::regex Rmemory_G("^[0-9]{1,15}$");
        if (!std::regex_match(memory, Rmemory_G)) {
          SLURMX_DEBUG("{} out of the range!\n{}", str, options.help());
          return 1;
        }
        memory_byte = (uint64_t)std::stoi(memory) * 1024;
        if (memory_byte == 0) {
          SLURMX_DEBUG("{} can not be zero!\n{}", str, options.help());
          return 1;
        }
      }
      if (memory_byte == 0) {
        SLURMX_DEBUG("{} can not be zero!\n{}", str, options.help());
        return 1;
      }
      return 0;
    }
  };

  if (MemoryParseClient("memory", result, parameter_bytes_memory, options)) {
    return 1;
  }
  client.allocatableResource.memory_limit_bytes = parameter_bytes_memory;

  if (MemoryParseClient("memory_swap", result, parameter_bytes_memory_swap,
                        options)) {
    return 1;
  }
  client.allocatableResource.memory_sw_limit_bytes =
      parameter_bytes_memory_swap;

  std::string str = result["task_executive_path"].as<std::string>();
  client.taskinfo.executive_path = str;

  for (auto arg : result["positional"].as<std::vector<std::string>>()) {
    client.taskinfo.arguments.push_back(arg);
  }

  std::string xdserver_address = result["Xdserver-address"].as<std::string>();
  std::string xdserver_port = result["Xdserver-port"].as<std::string>();
  std::string ctlXdserver_address =
      result["CtlXdserver-address"].as<std::string>();
  std::string ctlXdserver_port = result["CtXdserver-port"].as<std::string>();

  std::regex regex_addr(
      R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

  if (!std::regex_match(xdserver_address, regex_addr)) {
    SLURMX_DEBUG("Xdserver address is invalid.\n{}", options.help());
    return 1;
  }

  if (!std::regex_match(xdserver_port, regex_port)) {
    SLURMX_DEBUG("Xdserver port is invalid.\n{}", options.help());
    return 1;
  }

  if (!std::regex_match(ctlXdserver_address, regex_addr)) {
    SLURMX_DEBUG("CtXdserver address is invalid.\n{}", options.help());
    return 1;
  }

  if (!std::regex_match(ctlXdserver_port, regex_port)) {
    SLURMX_DEBUG("Xdserver port is invalid.\n{}", options.help());
    return 1;
  }

  std::string xdserver_addr_port =
      fmt::format("{}:{}", xdserver_address, xdserver_port);
  std::string ctlXdserver_addr_port =
      fmt::format("{}:{}", ctlXdserver_address, ctlXdserver_port);

  SlurmxErr err;
  err = client.Init(xdserver_addr_port, ctlXdserver_addr_port);
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("{}", SlurmxErrStr(err));
    return 1;
  }

  err = client.Run();
  shutdown.join();
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("{}", SlurmxErrStr(err));
    return 1;
  } else
    return 0;
}