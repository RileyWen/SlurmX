#include <cxxopts.hpp>
#include <iostream>
#include <numeric>
#include <regex>
#include <vector>

#include "PublicHeader.h"
#include "XdServer.h"

int main(int argc, char** argv) {
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif

  cxxopts::Options options("slurmxd");

  // clang-format off
  options.add_options()
    ("l,listen", "listening address",
        cxxopts::value<std::string>()->default_value("0.0.0.0"))
    ("p,port", "listening port",
        cxxopts::value<std::string>()->default_value("10010"))
    ("ncpu", "# of total cpu core", cxxopts::value<uint32_t>())
    ("nmem", "# of total memory in bytes", cxxopts::value<uint64_t>())
  ;
  // clang-format on

  auto parsed_args = options.parse(argc, argv);

  std::string address = parsed_args["listen"].as<std::string>();
  std::string port = parsed_args["port"].as<std::string>();

  std::regex regex_addr(
      R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

  if (!std::regex_match(address, regex_addr)) {
    fmt::print("Listening address is invalid.\n{}\n", options.help());
    return 1;
  }

  if (!std::regex_match(port, regex_port)) {
    fmt::print("Listening port is invalid.\n{}\n", options.help());
    return 1;
  }

  if (parsed_args.count("ncpu") == 0) {
    fmt::print("ncpu must be specified.\n{}\n", options.help());
    return 1;
  }

  if (parsed_args.count("nmem") == 0) {
    fmt::print("nmem must be specified.\n{}\n", options.help());
    return 1;
  }

  std::string server_listen_addr_port = fmt::format("{}:{}", address, port);

  resource_t resource_in_cmd;
  resource_in_cmd.cpu_count = parsed_args["ncpu"].as<uint32_t>();
  resource_in_cmd.memory_bytes = parsed_args["nmem"].as<uint64_t>();
  resource_in_cmd.memory_sw_bytes = parsed_args["nmem"].as<uint64_t>();

  g_server =
      std::make_unique<Xd::XdServer>(server_listen_addr_port, resource_in_cmd);

  g_server->Wait();

  return 0;
}