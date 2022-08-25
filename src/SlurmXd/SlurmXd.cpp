#include <absl/strings/str_split.h>
#include <event2/thread.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <boost/filesystem/string_file.hpp>
#include <cxxopts.hpp>
#include <filesystem>
#include <regex>

#include "CtlXdClient.h"
#include "XdPublicDefs.h"
#include "XdServer.h"
#include "slurmx/FdFunctions.h"
#include "slurmx/Network.h"
#include "slurmx/PublicHeader.h"
#include "slurmx/String.h"

using Xd::g_config;
using Xd::Node;
using Xd::Partition;

void ParseConfig(int argc, char** argv) {
  if (std::filesystem::exists(kDefaultConfigPath)) {
    try {
      YAML::Node config = YAML::LoadFile(kDefaultConfigPath);

      if (config["SlurmXdListen"])
        g_config.ListenConf.SlurmXdListenAddr =
            config["listen"].as<std::string>();
      else
        g_config.ListenConf.SlurmXdListenAddr = "0.0.0.0";

      // Todo: Add corresponding configuration field.

      g_config.ListenConf.SlurmXdListenPort = kXdDefaultPort;

      g_config.ListenConf.UnixSocketListenAddr =
          fmt::format("unix://{}", kDefaultSlurmXdUnixSockPath);

      if (config["UseTls"] && config["UseTls"].as<bool>()) {
        g_config.ListenConf.UseTls = true;
        if (config["CertFilePath"]) {
          g_config.ListenConf.CertFilePath =
              config["CertFilePath"].as<std::string>();

          try {
            boost::filesystem::load_string_file(
                g_config.ListenConf.CertFilePath,
                g_config.ListenConf.CertContent);
          } catch (const std::exception& e) {
            SLURMX_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.CertContent.empty()) {
            SLURMX_ERROR(
                "UseTls is true, but the file specified by CertFilePath is "
                "empty");
          }
        } else {
          SLURMX_ERROR("UseTls is true, but CertFilePath is empty");
          std::exit(1);
        }
        if (config["KeyFilePath"]) {
          g_config.ListenConf.KeyFilePath =
              config["KeyFilePath"].as<std::string>();

          try {
            boost::filesystem::load_string_file(g_config.ListenConf.KeyFilePath,
                                                g_config.ListenConf.KeyContent);
          } catch (const std::exception& e) {
            SLURMX_ERROR("Read key file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.KeyContent.empty()) {
            SLURMX_ERROR(
                "UseTls is true, but the file specified by KeyFilePath is "
                "empty");
            std::exit(1);
          }
        } else {
          SLURMX_ERROR("UseTls is true, but KeyFilePath is empty");
          std::exit(1);
        }
      } else {
        g_config.ListenConf.UseTls = false;
      }

      if (config["ControlMachine"]) {
        std::string addr;
        addr = config["ControlMachine"].as<std::string>();
        g_config.ControlMachine = fmt::format("{}:{}", addr, kCtlXdDefaultPort);
      } else
        std::exit(1);

      if (config["SlurmXdDebugLevel"])
        g_config.SlurmXdDebugLevel =
            config["SlurmXdDebugLevel"].as<std::string>();
      else
        std::exit(1);

      if (config["SlurmXdLogFile"])
        g_config.SlurmXdLogFile = config["SlurmXdLogFile"].as<std::string>();
      else
        g_config.SlurmXdLogFile = "/tmp/slurmxd/slurmxd.log";

      if (config["Nodes"]) {
        for (auto it = config["Nodes"].begin(); it != config["Nodes"].end();
             ++it) {
          auto node = it->as<YAML::Node>();
          auto node_ptr = std::make_shared<Node>();
          std::list<std::string> name_list;

          if (node["name"]) {
            std::list<std::string> str_list =
                absl::StrSplit(node["name"].Scalar(), ',');
            for (auto&& str : str_list) {
              std::string str_s{absl::StripAsciiWhitespace(str)};
              if (!util::ParseHostList(str_s, &name_list)) {
                SLURMX_ERROR("Illegal node name string format.");
                std::exit(1);
              }
            }

            SLURMX_TRACE("node name list parsed: {}",
                         fmt::join(name_list, ", "));
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

          for (auto&& name : name_list) {
            if (slurmx::IsAValidIpv4Address(name)) {
              SLURMX_INFO(
                  "Node name `{}` is a valid ipv4 address and doesn't "
                  "need resolving.",
                  name);
              g_config.Ipv4ToNodesHostname[name] = name;
              g_config.Nodes[name] = node_ptr;
            } else {
              std::string ipv4;
              if (!slurmx::ResolveIpv4FromHostname(name, &ipv4)) {
                SLURMX_ERROR("Init error: Cannot resolve hostname of `{}`",
                             name);
                std::exit(1);
              }
              SLURMX_INFO("Resolve hostname `{}` to `{}`", name, ipv4);
              g_config.Ipv4ToNodesHostname[ipv4] = name;

              g_config.Nodes[name] = node_ptr;
            }
          }
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
            name.assign(partition["name"].Scalar());
          } else
            std::exit(1);

          if (partition["nodes"]) {
            nodes = partition["nodes"].as<std::string>();
          } else
            std::exit(1);

          std::list<std::string> name_list;
          if (!util::ParseHostList(nodes, &name_list)) {
            SLURMX_ERROR("Illegal node name string format.");
            std::exit(1);
          }

          for (auto&& node : name_list) {
            std::string node_s{node};

            auto node_it = g_config.Nodes.find(node_s);
            if (node_it != g_config.Nodes.end()) {
              node_it->second->partition_name = name;
              part.nodes.emplace(node_it->first);
              SLURMX_INFO("Set the partition of node {} to {}", node_it->first,
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
      std::exit(1);
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
      std::exit(1);
    }

    g_config.ListenConf.SlurmXdListenAddr =
        parsed_args["listen"].as<std::string>();
    g_config.ListenConf.SlurmXdListenPort = kXdDefaultPort;

    if (parsed_args.count("server-address") == 0) {
      fmt::print("SlurmCtlXd address must be specified.\n{}\n", options.help());
      std::exit(1);
    }
    g_config.ControlMachine = parsed_args["server-address"].as<std::string>();
  }

  // Check the format of SlurmXdListen
  std::regex ipv4_re(R"(^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$)");
  std::smatch ivp4_match;
  if (!std::regex_match(g_config.ListenConf.SlurmXdListenAddr, ivp4_match,
                        ipv4_re)) {
    SLURMX_ERROR("Illegal CtlXd address format.");
    std::exit(1);
  }

  char hostname[HOST_NAME_MAX + 1];
  int r = gethostname(hostname, HOST_NAME_MAX + 1);
  if (r != 0) {
    SLURMX_ERROR("Error: get hostname.");
    std::exit(1);
  }
  g_config.Hostname.assign(hostname);

  auto node_it = g_config.Nodes.find(g_config.Hostname);
  if (node_it == g_config.Nodes.end()) {
    SLURMX_ERROR("This machine {} is not contained in Nodes!",
                 g_config.Hostname);
    std::exit(1);
  }

  SLURMX_INFO("Found this machine {} in Nodes", g_config.Hostname);

  uint32_t part_id, node_index;
  const std::string& part_name = node_it->second->partition_name;
  auto part_it = g_config.Partitions.find(part_name);
  if (part_it == g_config.Partitions.end()) {
    SLURMX_ERROR(
        "This machine {} belongs to {} partition, but the partition "
        "doesn't exist.",
        g_config.Hostname, part_name);
    std::exit(1);
  }
  part_id = std::distance(g_config.Partitions.begin(), part_it);

  auto node_it_in_part = part_it->second.nodes.find(g_config.Hostname);
  if (node_it_in_part == part_it->second.nodes.end()) {
    SLURMX_ERROR(
        "This machine {} can't be found in "
        "g_config.Partition[\"{}\"].nodes . Exit.",
        g_config.Hostname, part_name);
    std::exit(1);
  }
  node_index = std::distance(part_it->second.nodes.begin(), node_it_in_part);
  XdNodeId node_id{part_id, node_index};
  SLURMX_INFO("NodeId of this machine: {}", node_id);

  g_config.NodeId = node_id;
}

void InitSpdlog() {
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
}

void CreateRequiredDirectories() {
  // Create log and sh directory recursively.
  try {
    std::filesystem::create_directories(kDefaultSlurmXTempDir);
    std::filesystem::create_directories(kDefaultSlurmXScriptDir);

    std::filesystem::path log_path{g_config.SlurmXdLogFile};
    auto log_dir = log_path.parent_path();
    if (!log_dir.empty()) std::filesystem::create_directories(log_dir);
  } catch (const std::exception& e) {
    SLURMX_ERROR("Invalid SlurmXdLogFile path {}: {}", g_config.SlurmXdLogFile,
                 e.what());
  }
}

void GlobalVariableInit() {
  CreateRequiredDirectories();
  InitSpdlog();

  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  Xd::g_thread_pool = std::make_unique<BS::thread_pool>(
      std::thread::hardware_concurrency() / 2);

  g_task_mgr = std::make_unique<Xd::TaskManager>();

  g_ctlxd_client = std::make_unique<Xd::CtlXdClient>();
  g_ctlxd_client->SetNodeId(g_config.NodeId);
  g_ctlxd_client->InitChannelAndStub(g_config.ControlMachine);
}

void StartServer() {
  GlobalVariableInit();

  // Set FD_CLOEXEC on stdin, stdout, stderr
  util::SetCloseOnExecOnFdRange(STDIN_FILENO, STDERR_FILENO + 1);

  g_server = std::make_unique<Xd::XdServer>(g_config.ListenConf);

  g_server->Wait();

  // Free global variables
  g_task_mgr.reset();
  g_server.reset();
  g_ctlxd_client.reset();

  std::exit(0);
}

void StartDaemon() {
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

  // If config parsing fails, this function will not return and will call
  // std::exit(1) instead.
  ParseConfig(argc, argv);

  if (g_config.SlurmXdForeground)
    StartServer();
  else
    StartDaemon();

  return 0;
}