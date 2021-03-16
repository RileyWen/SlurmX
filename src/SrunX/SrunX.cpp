#include "PublicHeader.h"
#include "SrunXClient.h"

std::unique_ptr<grpc::ClientReaderWriter<SrunXStreamRequest, SrunXStreamReply>>
    SrunXClient::m_stream_;
std::mutex SrunXClient::m_cv_m_;
std::condition_variable SrunXClient::m_cv_;
std::atomic_int SrunXClient::m_signal_fg_;
std::atomic_int SrunXClient::m_exit_fg_;

int main(int argc, char **argv) {
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif
  try {
    SrunXClient client;

    cxxopts::Options options("./srunx", " - srunX command line options");
    options.positional_help("<remote path> [Args...]").show_positional_help();
    options.custom_help(
        "<--cpu <value>> <--memory <value>[MmGg]> <--memory_swap "
        "<value>[MmGg]>\n"
        "\t\t\t<--xdserver-address <address>> <--xdserver-port <port>> "
        "<--ctlxdserver-address <address>>\n"
        "\t\t\t<--ctlxdserver-port <port>>");
    options.add_options()("s,xdserver-address", "SlurmXd server address",
                          cxxopts::value<std::string>())(
        "p,xdserver-port", "SlurmXd server port",
        cxxopts::value<std::string>())("S,ctlxdserver-address",
                                       "SlurmCtlXd server address",
                                       cxxopts::value<std::string>())(
        "P,ctlxdserver-port", "SlurmCtlXd server port",
        cxxopts::value<std::string>())("c,cpu",
                                       "limiting the cpu usage of task(cores) ",
                                       cxxopts::value<uint64_t>())(
        "m,memory",
        "limiting the memory usage of task (default:Kb <value>[MmGg] eg:128m)",
        cxxopts::value<std::string>())("w,memory_swap",
                                       "limiting the swap memory usage of task "
                                       "(default:Kb <value>[MmGg] eg:128m)",
                                       cxxopts::value<std::string>())(
        "e,executive-path", "task executive path",
        cxxopts::value<std::string>())("h,help", "Print help")(
        "task-arguments",
        "task arguments: these are the arguments that are entered "
        "for executive path task",
        cxxopts::value<std::vector<std::string>>());

    options.parse_positional({"executive-path", "task-arguments"});
    auto result = options.parse(argc, argv);
    if (result.count("help")) {
      fmt::print("{}\n", options.help());
      return 0;
    }

    if (result.count("cpu") == 0) {
      SLURMX_WARN("cpu must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("memory") == 0) {
      SLURMX_WARN("memory must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("memory_swap") == 0) {
      SLURMX_WARN("memory_swap must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("executive-path") == 0) {
      SLURMX_WARN("executive path must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("xdserver-address") == 0) {
      SLURMX_WARN("SlurmCtlXd address must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("ctlxdserver-address") == 0) {
      SLURMX_WARN("SlurmCtlXd address must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("xdserver-port") == 0) {
      SLURMX_WARN("SlurmXd port must be specified.\n{}", options.help());
      return 1;
    }
    if (result.count("ctlxdserver-port") == 0) {
      SLURMX_WARN("SlurmXd port must be specified.\n{}", options.help());
      return 1;
    }

    uint64_t parameter_bytes_cpu;
    uint64_t parameter_bytes_memory;
    uint64_t parameter_bytes_memory_swap;

    parameter_bytes_cpu = result["cpu"].as<uint64_t>();
    if (parameter_bytes_cpu == 0) {
      SLURMX_WARN("Cpu core can not be zero!\n{}", options.help());
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
        SLURMX_WARN(
            "{} must be uint number or the uint number ends with "
            "'m/M/g/G'!\n{}",
            str, options.help());
        return 1;
      } else {
        if (memory[memory.length() - 1] == 'M' ||
            memory[memory.length() - 1] == 'm') {
          std::regex Rmemory_M("^[0-9]{1,8}[mMgG]?$");
          if (!std::regex_match(memory, Rmemory_M)) {
            SLURMX_WARN("{} out of the range!\n{}", str, options.help());
            return 1;
          }
          memory_byte =
              (uint64_t)std::stoi(memory.substr(0, memory.length() - 1)) *
              1024 * 1024;
        } else if (memory[memory.length() - 1] == 'G' ||
                   memory[memory.length() - 1] == 'g') {
          std::regex Rmemory_G("^[0-9]{1,5}[mMgG]?$");
          if (!std::regex_match(memory, Rmemory_G)) {
            SLURMX_WARN("{} out of the range!\n{}", str, options.help());
            return 1;
          }
          memory_byte =
              (uint64_t)std::stoi(memory.substr(0, memory.length() - 1)) *
              1024 * 1024 * 1024;
        } else {
          std::regex Rmemory_G("^[0-9]{1,15}$");
          if (!std::regex_match(memory, Rmemory_G)) {
            SLURMX_WARN("{} out of the range!\n{}", str, options.help());
            return 1;
          }
          memory_byte = (uint64_t)std::stoi(memory) * 1024;
          if (memory_byte == 0) {
            SLURMX_WARN("{} can not be zero!\n{}", str, options.help());
            return 1;
          }
        }
        if (memory_byte == 0) {
          SLURMX_WARN("{} can not be zero!\n{}", str, options.help());
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

    std::string str = result["executive-path"].as<std::string>();
    client.taskinfo.executive_path = str;
    if (result.count("task-arguments") != 0) {
      for (auto arg : result["task-arguments"].as<std::vector<std::string>>()) {
        client.taskinfo.arguments.push_back(arg);
      }
    }
    std::string xdserver_address = result["xdserver-address"].as<std::string>();
    std::string xdserver_port = result["xdserver-port"].as<std::string>();
    std::string ctlXdserver_address =
        result["ctlxdserver-address"].as<std::string>();
    std::string ctlXdserver_port = result["ctlxdserver-port"].as<std::string>();

    std::regex regex_addr(
        R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

    std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                          R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

    if (!std::regex_match(xdserver_address, regex_addr)) {
      SLURMX_WARN("Xdserver address is invalid.\n{}", options.help());
      return 1;
    }

    if (!std::regex_match(xdserver_port, regex_port)) {
      SLURMX_WARN("Xdserver port is invalid.\n{}", options.help());
      return 1;
    }

    if (!std::regex_match(ctlXdserver_address, regex_addr)) {
      SLURMX_WARN("CtXdserver address is invalid.\n{}", options.help());
      return 1;
    }

    if (!std::regex_match(ctlXdserver_port, regex_port)) {
      SLURMX_WARN("Xdserver port is invalid.\n{}", options.help());
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
    client.Wait();

    if (err != SlurmxErr::kOk) {
      SLURMX_ERROR("{}", SlurmxErrStr(err));
      return 1;
    } else
      return 0;
  } catch (cxxopts::option_not_exists_exception e) {
    SLURMX_ERROR("{}", e.what());
    return 0;
  }
}