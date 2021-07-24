#include <cxxopts.hpp>
#include <iostream>
#include <numeric>
#include <regex>
#include <vector>

#include "CtlXdClient.h"
#include "PublicHeader.h"
#include "XdServer.h"

int main(int argc, char** argv) {
  // Todo: Add single program instance checking.

  cxxopts::Options options("slurmxd");

  // clang-format off
  options.add_options()
    ("l,listen", "Listen address format: <IP>:<port>",
        cxxopts::value<std::string>()->default_value(fmt::format("0.0.0.0:{}", kXdDefaultPort)))
    ("s,server-address", "SlurmCtlXd address format: <IP>:<port>",
        cxxopts::value<std::string>())
    ("c,ncpu", "# of total cpu core", cxxopts::value<uint32_t>())
    ("m,memory", R"(# of total memory. Format: \d+[BKMG])", cxxopts::value<std::string>())
    ("p,partition", "Name of the partition", cxxopts::value<std::string>())
    ("D,debug-level", "[trace|debug|info|warn|error]", cxxopts::value<std::string>()->default_value("info"))
    ("h,help", "Show help")
  ;
  // clang-format on

  auto parsed_args = options.parse(argc, argv);

  // Todo: Check static level setting.
  const std::string& debug_level = parsed_args["debug-level"].as<std::string>();
  if (debug_level == "trace")
    spdlog::set_level(spdlog::level::trace);
  else if (debug_level == "debug")
    spdlog::set_level(spdlog::level::debug);
  else if (debug_level == "info")
    spdlog::set_level(spdlog::level::info);
  else if (debug_level == "warn")
    spdlog::set_level(spdlog::level::warn);
  else if (debug_level == "error")
    spdlog::set_level(spdlog::level::err);
  else {
    SLURMX_ERROR("Illegal debug-level format.");
    return 1;
  }

  if (parsed_args.count("help") > 0) {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (parsed_args.count("ncpu") == 0) {
    fmt::print("ncpu must be specified.\n{}\n", options.help());
    return 1;
  }

  if (parsed_args.count("memory") == 0) {
    fmt::print("memory must be specified.\n{}\n", options.help());
    return 1;
  }

  if (parsed_args.count("server-address") == 0) {
    fmt::print("SlurmCtlXd address must be specified.\n{}\n", options.help());
    return 1;
  }

  if (parsed_args.count("partition") == 0) {
    fmt::print("SlurmXd must belong to one partition.\n{}\n", options.help());
    return 1;
  }

  const std::string& partition_name =
      parsed_args["partition"].as<std::string>();
  if (partition_name == "") {
    fmt::print("Partition name should not be empty!\n{}\n", options.help());
    return 1;
  }

  const std::string& memory = parsed_args["memory"].as<std::string>();
  std::regex mem_regex(R"((\d+)([KMBG]))");
  std::smatch mem_group;
  if (!std::regex_search(memory, mem_group, mem_regex)) {
    SLURMX_ERROR("Illegal memory format.");
    return 1;
  }

  uint64_t memory_bytes = std::stoul(mem_group[1]);
  if (mem_group[2] == "K")
    memory_bytes *= 1024;
  else if (mem_group[2] == "M")
    memory_bytes *= 1024 * 1024;
  else if (mem_group[2] == "G")
    memory_bytes *= 1024 * 1024 * 1024;

  AllocatableResource resource_in_cmd;
  resource_in_cmd.cpu_count = parsed_args["ncpu"].as<uint32_t>();
  resource_in_cmd.memory_bytes = memory_bytes;
  resource_in_cmd.memory_sw_bytes = memory_bytes;

  SlurmxErr err;

  g_task_mgr = std::make_unique<Xd::TaskManager>();

  g_ctlxd_client = std::make_unique<Xd::CtlXdClient>();
  err =
      g_ctlxd_client->Connect(parsed_args["server-address"].as<std::string>());
  if (err == SlurmxErr::kConnectionTimeout) {
    SLURMX_ERROR("Cannot connect to SlurmCtlXd.");
    return 1;
  }

  // Todo: Set FD_CLOEXEC on stdin, stdout, stderr

  const std::string& listen_addr = parsed_args["listen"].as<std::string>();
  g_server = std::make_unique<Xd::XdServer>(listen_addr, resource_in_cmd);

  std::regex addr_port_re(R"(^.*:(\d+)$)");
  std::smatch port_group;
  if (!std::regex_match(listen_addr, port_group, addr_port_re)) {
    SLURMX_ERROR("Illegal CtlXd address format.");
    return 1;
  }

  err = g_ctlxd_client->RegisterOnCtlXd(partition_name, resource_in_cmd,
                                        std::stoul(port_group[1]));
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("Exit due to registration error... Shutting down server...");
    g_server->Shutdown();
    g_server->Wait();
    return 1;
  }

  g_server->Wait();

  // Free global variables
  g_task_mgr.reset();
  g_server.reset();
  g_ctlxd_client.reset();

  return 0;
}