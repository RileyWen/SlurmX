#include <event2/thread.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <yaml-cpp/yaml.h>

#include <cxxopts.hpp>
#include <filesystem>
#include <iostream>
#include <regex>

#include "CtlXdClient.h"
#include "XdServer.h"
#include "slurmx/PublicHeader.h"

struct Config {
  std::string listen_addr;
  std::string server_addr;
  std::string cpu;
  std::string memory;
  std::string partition;
  std::string debug_level;
  std::string log_path;
};

Config g_config;

void GlobalVariableInit() {
  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      g_config.log_path, 1048576 * 5, 3);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

  spdlog::init_thread_pool(8192, 1);
  auto logger = std::make_shared<spdlog::async_logger>(
      "default", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  spdlog::set_default_logger(logger);

  if (g_config.debug_level == "trace")
    spdlog::set_level(spdlog::level::trace);
  else if (g_config.debug_level == "debug")
    spdlog::set_level(spdlog::level::debug);
  else if (g_config.debug_level == "info")
    spdlog::set_level(spdlog::level::info);
  else if (g_config.debug_level == "warn")
    spdlog::set_level(spdlog::level::warn);
  else if (g_config.debug_level == "error")
    spdlog::set_level(spdlog::level::err);
  else {
    SLURMX_ERROR("Illegal debug-level format.");
    std::exit(1);
  }

  spdlog::flush_on(spdlog::level::err);

  g_task_mgr = std::make_unique<Xd::TaskManager>();

  g_ctlxd_client = std::make_unique<Xd::CtlXdClient>();
}

int main(int argc, char** argv) {
  // Todo: Add single program instance checking.

  if (std::filesystem::exists(kDefaultConfigPath)) {
    try {
      YAML::Node root = YAML::LoadFile(kDefaultConfigPath);

      if (root["SlurmXd"]) {
        YAML::Node config = root["SlurmXd"];
        if (config["listen"])
          g_config.listen_addr = config["listen"].as<std::string>();
        else
          g_config.listen_addr = fmt::format("0.0.0.0:{}", kXdDefaultPort);

        if (config["server-address"])
          g_config.server_addr = config["server-address"].as<std::string>();
        else
          return 1;

        if (config["cpu"])
          g_config.cpu = config["cpu"].as<std::string>();
        else
          return 1;

        if (config["memory"])
          g_config.memory = config["memory"].as<std::string>();
        else
          return 1;

        if (config["partition"])
          g_config.partition = config["partition"].as<std::string>();
        else
          return 1;

        if (config["debug-level"])
          g_config.debug_level = config["debug-level"].as<std::string>();
        else
          return 1;

        if (config["log-path"])
          g_config.log_path = config["log-path"].as<std::string>();
        else
          g_config.log_path = "/tmp/slurmxd.log";
      }
    } catch (YAML::BadFile& e) {
      SLURMX_ERROR("Can't open config file {}: {}", kDefaultConfigPath,
                   e.what());
      return 1;
    }
  } else {
    cxxopts::Options options("slurmxd");

    // clang-format off
    options.add_options()
        ("l,listen", "Listen address format: <IP>:<port>",
         cxxopts::value<std::string>()->default_value(fmt::format("0.0.0.0:{}", kXdDefaultPort)))
        ("s,server-address", "SlurmCtlXd address format: <IP>:<port>",
         cxxopts::value<std::string>())
        ("c,cpu", "# of total cpu core", cxxopts::value<std::string>())
        ("m,memory", R"(# of total memory. Format: \d+[BKMG])", cxxopts::value<std::string>())
        ("p,partition", "Name of the partition", cxxopts::value<std::string>())
        ("D,debug-level", "[trace|debug|info|warn|error]", cxxopts::value<std::string>()->default_value("info"))
        ("h,help", "Show help")
        ;
    // clang-format on

    auto parsed_args = options.parse(argc, argv);

    // Todo: Check static level setting.
    g_config.debug_level = parsed_args["debug-level"].as<std::string>();

    if (parsed_args.count("help") > 0) {
      fmt::print("{}\n", options.help());
      return 0;
    }

    if (parsed_args.count("cpu") == 0) {
      fmt::print("cpu must be specified.\n{}\n", options.help());
      return 1;
    }
    g_config.cpu = parsed_args["cpu"].as<std::string>();

    if (parsed_args.count("memory") == 0) {
      fmt::print("memory must be specified.\n{}\n", options.help());
      return 1;
    }
    g_config.memory = parsed_args["memory"].as<std::string>();

    g_config.listen_addr = parsed_args["listen"].as<std::string>();

    if (parsed_args.count("server-address") == 0) {
      fmt::print("SlurmCtlXd address must be specified.\n{}\n", options.help());
      return 1;
    }
    g_config.server_addr = parsed_args["server-address"].as<std::string>();

    if (parsed_args.count("partition") == 0) {
      fmt::print("SlurmXd must belong to one partition.\n{}\n", options.help());
      return 1;
    }
    g_config.partition = parsed_args["partition"].as<std::string>();
  }

  if (g_config.partition.empty()) {
    fmt::print("Partition name should not be empty!\n");
    return 1;
  }

  std::regex mem_regex(R"((\d+)([KMBG]))");
  std::smatch mem_group;
  if (!std::regex_search(g_config.memory, mem_group, mem_regex)) {
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
  resource_in_cmd.cpu_count = std::stoul(g_config.cpu);
  resource_in_cmd.memory_bytes = memory_bytes;
  resource_in_cmd.memory_sw_bytes = memory_bytes;

  GlobalVariableInit();

  SlurmxErr err;

  err = g_ctlxd_client->Connect(g_config.server_addr);
  if (err == SlurmxErr::kConnectionTimeout) {
    SLURMX_ERROR("Cannot connect to SlurmCtlXd.");
    return 1;
  }

  // Todo: Set FD_CLOEXEC on stdin, stdout, stderr

  g_server = std::make_unique<Xd::XdServer>(g_config.listen_addr);

  std::regex addr_port_re(R"(^.*:(\d+)$)");
  std::smatch port_group;
  if (!std::regex_match(g_config.listen_addr, port_group, addr_port_re)) {
    SLURMX_ERROR("Illegal CtlXd address format.");
    return 1;
  }

  err = g_ctlxd_client->RegisterOnCtlXd(g_config.partition, resource_in_cmd,
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