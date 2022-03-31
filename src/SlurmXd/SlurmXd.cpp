#include <absl/strings/str_split.h>
#include <event2/thread.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <cxxopts.hpp>
#include <filesystem>
#include <iostream>
#include <regex>

#include "CtlXdClient.h"
#include "XdServer.h"
#include "slurmx/PublicHeader.h"
#include "slurmx/StringParse.h"

struct Node {
  uint32_t cpu;
  uint64_t memory_bytes;

  std::string partition_name;
};

struct Partition {
  std::unordered_set<std::string> nodes;
  std::unordered_set<std::string> AllowAccounts;
};

struct Config {
  std::string SlurmXdListen;
  std::string ControlMachine;
  std::string SlurmXdDebugLevel;
  std::string SlurmXdLogFile;

  bool SlurmXdForeground{};

  std::string Hostname;
  std::unordered_map<std::string, std::shared_ptr<Node>> Nodes;
  std::unordered_map<std::string, Partition> Partitions;
};

Config g_config;

void GlobalVariableInit() {
  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  if (g_config.SlurmXdDebugLevel == "trace")
    spdlog::set_level(spdlog::level::trace);
  else if (g_config.SlurmXdDebugLevel == "debug")
    spdlog::set_level(spdlog::level::debug);
  else if (g_config.SlurmXdDebugLevel == "info")
    spdlog::set_level(spdlog::level::info);
  else if (g_config.SlurmXdDebugLevel == "warn")
    spdlog::set_level(spdlog::level::warn);
  else if (g_config.SlurmXdDebugLevel == "error")
    spdlog::set_level(spdlog::level::err);
  else {
    SLURMX_ERROR("Illegal debug-level format.");
    std::exit(1);
  }

  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      g_config.SlurmXdLogFile, 1048576 * 5, 3);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

  if (g_config.SlurmXdDebugLevel == "trace") {
    file_sink->set_level(spdlog::level::trace);
    console_sink->set_level(spdlog::level::trace);
  } else if (g_config.SlurmXdDebugLevel == "debug") {
    file_sink->set_level(spdlog::level::debug);
    console_sink->set_level(spdlog::level::debug);
  } else if (g_config.SlurmXdDebugLevel == "info") {
    file_sink->set_level(spdlog::level::info);
    console_sink->set_level(spdlog::level::info);
  } else if (g_config.SlurmXdDebugLevel == "warn") {
    file_sink->set_level(spdlog::level::warn);
    console_sink->set_level(spdlog::level::warn);
  } else if (g_config.SlurmXdDebugLevel == "error") {
    file_sink->set_level(spdlog::level::err);
    console_sink->set_level(spdlog::level::err);
  } else {
    SLURMX_ERROR("Illegal debug-level format.");
    std::exit(1);
  }

  spdlog::init_thread_pool(256, 1);
  auto logger = std::make_shared<spdlog::async_logger>(
      "default", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  spdlog::set_default_logger(logger);

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));

  spdlog::set_level(spdlog::level::trace);

  g_task_mgr = std::make_unique<Xd::TaskManager>();

  g_ctlxd_client = std::make_unique<Xd::CtlXdClient>();
}

void StartServer() {
  auto node_it = g_config.Nodes.find(g_config.Hostname);
  if (node_it == g_config.Nodes.end()) {
    SLURMX_ERROR("This machine {} is not contained in Nodes!",
                 g_config.Hostname);
    std::exit(1);
  }

  SLURMX_INFO("Found this machine {} in Nodes", g_config.Hostname);

  AllocatableResource resource_in_cmd;
  resource_in_cmd.cpu_count = node_it->second->cpu;
  resource_in_cmd.memory_bytes = node_it->second->memory_bytes;
  resource_in_cmd.memory_sw_bytes = node_it->second->memory_bytes;

  GlobalVariableInit();

  SlurmxErr err;

  err = g_ctlxd_client->Connect(g_config.ControlMachine);
  if (err == SlurmxErr::kConnectionTimeout) {
    SLURMX_ERROR("Cannot connect to SlurmCtlXd.");
    std::exit(1);
  }

  // Todo: Set FD_CLOEXEC on stdin, stdout, stderr

  g_server = std::make_unique<Xd::XdServer>(g_config.SlurmXdListen);

  std::regex addr_port_re(R"(^.*:(\d+)$)");
  std::smatch port_group;
  if (!std::regex_match(g_config.SlurmXdListen, port_group, addr_port_re)) {
    SLURMX_ERROR("Illegal CtlXd address format.");
    std::exit(1);
  }

  err = g_ctlxd_client->RegisterOnCtlXd(std::stoul(port_group[1]));
  if (err != SlurmxErr::kOk) {
    SLURMX_ERROR("Exit due to registration error... Shutting down server...");
    g_server->Shutdown();
    g_server->Wait();
    std::exit(1);
  }

  g_server->Wait();

  // Free global variables
  g_task_mgr.reset();
  g_server.reset();
  g_ctlxd_client.reset();

  std::exit(0);
}

void StartDeamon() {
  /* Our process ID and Session ID */
  pid_t pid, sid;

  /* Fork off the parent process */
  pid = fork();
  if (pid < 0) {
    SLURMX_ERROR("Error: fork()");
    exit(1);
  }
  /* If we got a good PID, then
     we can exit the parent process. */
  if (pid > 0) {
    exit(0);
  }

  /* Change the file mode mask */
  umask(0);

  /* Open any logs here */

  /* Create a new SID for the child process */
  sid = setsid();
  if (sid < 0) {
    /* Log the failure */
    SLURMX_ERROR("Error: setsid()");
    exit(1);
  }

  /* Change the current working directory */
  if ((chdir("/")) < 0) {
    SLURMX_ERROR("Error: chdir()");
    /* Log the failure */
    exit(1);
  }

  /* Close out the standard file descriptors */
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  /* Daemon-specific initialization goes here */
  StartServer();

  exit(EXIT_SUCCESS);
}

int main(int argc, char** argv) {
  // Todo: Add single program instance checking.

  if (std::filesystem::exists(kDefaultConfigPath)) {
    try {
      YAML::Node config = YAML::LoadFile(kDefaultConfigPath);

      if (config["SlurmXdListen"])
        g_config.SlurmXdListen = config["listen"].as<std::string>();
      else
        g_config.SlurmXdListen = fmt::format("0.0.0.0:{}", kXdDefaultPort);

      if (config["ControlMachine"]) {
        std::string addr;
        addr = config["ControlMachine"].as<std::string>();
        g_config.ControlMachine = fmt::format("{}:{}", addr, kCtlXdDefaultPort);
      } else
        return 1;

      if (config["SlurmXdDebugLevel"])
        g_config.SlurmXdDebugLevel =
            config["SlurmXdDebugLevel"].as<std::string>();
      else
        return 1;

      if (config["SlurmXdLogFile"])
        g_config.SlurmXdLogFile = config["SlurmXdLogFile"].as<std::string>();
      else
        g_config.SlurmXdLogFile = "/tmp/slurmxd.log";

      if (config["Nodes"]) {
        for (auto it = config["Nodes"].begin(); it != config["Nodes"].end();
             ++it) {
          auto node = it->as<YAML::Node>();
          auto node_ptr = std::make_shared<Node>();
          std::list<std::string> name_list;

          if (node["name"]) {
            if(!util::ParseHostList(node["name"].Scalar(),&name_list)){
              SLURMX_ERROR("Illegal node name string format.");
              std::exit(1);
            }
            SLURMX_TRACE("node name list parsed: {}", fmt::join(name_list,", "));
          } else
            std::exit(1);

          if (node["cpu"])
            node_ptr->cpu = std::stoul(node["cpu"].as<std::string>());
          else
            std::exit(1);

          if (node["memory"]) {
            auto memory = node["memory"].as<std::string>();
            std::regex mem_regex(R"((\d+)([KMBG]))");
            std::smatch mem_group;
            if (!std::regex_search(memory, mem_group, mem_regex)) {
              SLURMX_ERROR("Illegal memory format.");
              std::exit(1);
            }

            uint64_t memory_bytes = std::stoul(mem_group[1]);
            if (mem_group[2] == "K")
              memory_bytes *= 1024;
            else if (mem_group[2] == "M")
              memory_bytes *= 1024 * 1024;
            else if (mem_group[2] == "G")
              memory_bytes *= 1024 * 1024 * 1024;

            node_ptr->memory_bytes = memory_bytes;
          } else
            std::exit(1);

          for(auto && name : name_list)
            g_config.Nodes[name] = node_ptr;
        }
      }

      if (config["Partitions"]) {
        for (auto it = config["Partitions"].begin();
             it != config["Partitions"].end(); ++it) {
          auto partition = it->as<YAML::Node>();
          std::string name;
          std::string nodes;
          Partition part;

          if (partition["name"]) {
            name.append(partition["name"].Scalar());
          } else
            std::exit(1);

          if (partition["nodes"]) {
            nodes = partition["nodes"].as<std::string>();
          } else
            std::exit(1);

          std::list<std::string> name_list;
          if(!util::ParseHostList(nodes, &name_list)){
            SLURMX_ERROR("Illegal node name string format.");
            std::exit(1);
          }

          for (auto&& node : name_list) {
            std::string node_s{node};

            auto node_it = g_config.Nodes.find(node_s);
            if (node_it != g_config.Nodes.end()) {
              node_it->second->partition_name = name;
              part.nodes.emplace(node_it->first);
              SLURMX_TRACE("Set the partition of node {} to {}", node_it->first,
                           name);
            }
          }

          g_config.Partitions.emplace(std::move(name), std::move(part));
        }

        if (config["SlurmXdForeground"]) {
          auto val = config["SlurmXdForeground"].as<std::string>();
          if (val == "true")
            g_config.SlurmXdForeground = true;
          else
            g_config.SlurmXdForeground = false;
        }
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
    g_config.SlurmXdDebugLevel = parsed_args["debug-level"].as<std::string>();

    if (parsed_args.count("help") > 0) {
      fmt::print("{}\n", options.help());
      return 0;
    }

    g_config.SlurmXdListen = parsed_args["listen"].as<std::string>();

    if (parsed_args.count("server-address") == 0) {
      fmt::print("SlurmCtlXd address must be specified.\n{}\n", options.help());
      return 1;
    }
    g_config.ControlMachine = parsed_args["server-address"].as<std::string>();
  }

  char hostname[HOST_NAME_MAX + 1];
  int r = gethostname(hostname, HOST_NAME_MAX + 1);
  if (r != 0) {
    SLURMX_ERROR("Error: get hostname.");
    std::exit(1);
  }
  g_config.Hostname.assign(hostname);

  if (g_config.SlurmXdForeground)
    StartServer();
  else
    StartDeamon();

  return 0;
}