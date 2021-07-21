#include "CommandLineParse.h"

namespace SrunX {

cxxopts::Options InitOptions() {
  cxxopts::Options options("./srunx", " - srunX command line options");
  options.positional_help("<remote path> [Args...]").show_positional_help();
  options.custom_help(
      "[--help] <--cpu <value>> <--memory <value>[MmGg]>\n"
      "\t  <--memory_swap <value>[MmGg]>  <--ctlxdserver-address <address>> \n"
      "\t  <--ctlxdserver-port <port>>");

  // clang-format off
  options.add_options()
      ("c,cpu", "Limiting the cpu usage of task(cores) ",
       cxxopts::value<uint64_t>())
      ("m,memory",
       "Limiting the memory usage of task (default:Kb <value>[MmGg] eg:128m)",
       cxxopts::value<std::string>())
      ("w,memory_swap",
       "Limiting the swap memory usage of task (default:Kb <value>[MmGg] eg:128m)",
       cxxopts::value<std::string>())
      ("S,ctlxd-address", "SlurmCtlXd server address",
       cxxopts::value<std::string>())
      ("P,ctlxd-port", "SlurmCtlXd server port",
       cxxopts::value<std::string>())
      ("positional-args", "Positional parameters",
       cxxopts::value<std::vector<std::string>>())
      ("h,help", "Print help")
  ;
  // clang-format on

  options.parse_positional({"positional-args"});

  return options;
}

SlurmxErr CheckValidityOfCommandLineArgs(
    const cxxopts::Options options, const cxxopts::ParseResult& parse_result) {
  try {
    if (parse_result.count("help") > 0) {
      fmt::print("{}\n", options.help());
      return SlurmxErr::kGenericFailure;
    }

    if (parse_result.count("cpu") == 0) {
      fmt::print("cpu must be specified.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }
    uint64_t cpu_count = parse_result["cpu"].as<uint64_t>();
    if (cpu_count == 0) {
      fmt::print("The # of cpu cores shall not be zero!\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    if (parse_result.count("memory") == 0) {
      fmt::print("memory must be specified.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    if (parse_result.count("memory_swap") == 0) {
      fmt::print("memory_swap must be specified.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    std::regex memory_regex("^[1-9][0-9]*[mMgGkK]$");

    const auto& memory = parse_result["memory"].as<std::string>();
    if (!std::regex_match(memory, memory_regex)) {
      fmt::print("Invalid memory expression", options.help());
      return SlurmxErr::kGenericFailure;
    }

    const auto& memory_sw = parse_result["memory_swap"].as<std::string>();
    if (!std::regex_match(memory_sw, memory_regex)) {
      fmt::print("Invalid memory_swap expression", options.help());
      return SlurmxErr::kGenericFailure;
    }

    if (parse_result.count("ctlxd-address") == 0) {
      fmt::print("SlurmCtlXd address must be specified.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    if (parse_result.count("ctlxd-port") == 0) {
      fmt::print("SlurmCtlXd port must be specified.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    const auto& ctlxd_server_address =
        parse_result["ctlxd-address"].as<std::string>();
    const auto& ctlxd_server_port =
        parse_result["ctlxd-port"].as<std::string>();

    std::regex regex_addr(
        R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

    std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                          R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

    if (!std::regex_match(ctlxd_server_address, regex_addr)) {
      fmt::print("CtlXd server address is invalid.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    if (!std::regex_match(ctlxd_server_port, regex_port)) {
      fmt::print("CtlXd server port is invalid.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

    // positional-args: <executive path> [Args...]
    if (parse_result.count("positional-args") == 0) {
      fmt::print("executive path must be specified.\n{}", options.help());
      return SlurmxErr::kGenericFailure;
    }

  } catch (const cxxopts::OptionException& e) {
    fmt::print("Command line argument error: {}\n", e.what());
    return SlurmxErr::kGenericFailure;
  }

  return SlurmxErr::kOk;
}

SlurmxErr ParseCommandLineArgsIntoStruct(
    const cxxopts::ParseResult& parse_result, CommandLineArgs* args) {
  args->required_resource.cpu_count = parse_result["cpu"].as<uint64_t>();

  char unit;
  ulong val;

  auto convert_memory_into_bytes = [](char unit, ulong val) -> uint64_t {
    constexpr uint64_t KB = 1024;
    constexpr uint64_t MB = 1024 * KB;
    constexpr uint64_t GB = 1024 * MB;
    switch (unit) {
      case 'K':
      case 'k':
        return val * KB;
      case 'M':
      case 'm':
        return val * MB;
      case 'G':
      case 'g':
        return val * GB;
      default:
        SLURMX_ERROR("Should not reach here!");
        return 0;
    }
  };

  const auto& memory = parse_result["memory"].as<std::string>();
  unit = memory.at(memory.size() - 1);
  val = std::stoul(memory.substr(0, memory.size() - 1));
  args->required_resource.memory_bytes = convert_memory_into_bytes(unit, val);

  const auto& memory_swap = parse_result["memory_swap"].as<std::string>();
  unit = memory_swap.at(memory.size() - 1);
  val = std::stoul(memory_swap.substr(0, memory_swap.size() - 1));
  args->required_resource.memory_sw_bytes =
      convert_memory_into_bytes(unit, val);

  const auto& positional_args =
      parse_result["positional-args"].as<std::vector<std::string>>();
  args->executive_path = positional_args[0];

  if (positional_args.size() > 1) {
    for (auto iter = positional_args.begin() + 1; iter != positional_args.end();
         iter++) {
      args->arguments.push_back(*iter);
    }
  }

  args->ctlxd_address = parse_result["ctlxd-address"].as<std::string>();
  args->ctlxd_port = parse_result["ctlxd-port"].as<std::string>();

  return SlurmxErr::kOk;
}

}  // namespace SrunX