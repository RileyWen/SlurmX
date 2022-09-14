#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>
#include <event2/thread.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <boost/filesystem/string_file.hpp>
#include <condition_variable>
#include <cxxopts.hpp>
#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "CtlXdGrpcServer.h"
#include "DbClient.h"
#include "TaskScheduler.h"
#include "XdNodeKeeper.h"
#include "XdNodeMetaContainer.h"
#include "slurmx/PublicHeader.h"
#include "slurmx/String.h"

void InitializeCtlXdGlobalVariables() {
  using namespace CtlXd;

  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      g_config.SlurmCtlXdLogFile, 1048576 * 5, 3);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

  if (g_config.SlurmCtlXdDebugLevel == "trace") {
    file_sink->set_level(spdlog::level::trace);
    console_sink->set_level(spdlog::level::trace);
  } else if (g_config.SlurmCtlXdDebugLevel == "debug") {
    file_sink->set_level(spdlog::level::debug);
    console_sink->set_level(spdlog::level::debug);
  } else if (g_config.SlurmCtlXdDebugLevel == "info") {
    file_sink->set_level(spdlog::level::info);
    console_sink->set_level(spdlog::level::info);
  } else if (g_config.SlurmCtlXdDebugLevel == "warn") {
    file_sink->set_level(spdlog::level::warn);
    console_sink->set_level(spdlog::level::warn);
  } else if (g_config.SlurmCtlXdDebugLevel == "error") {
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

  char hostname[HOST_NAME_MAX + 1];
  int err = gethostname(hostname, HOST_NAME_MAX + 1);
  if (err != 0) {
    SLURMX_ERROR("Error: get hostname.");
    std::exit(1);
  }

  g_config.Hostname.assign(hostname);
  SLURMX_INFO("Hostname of slurmctlxd: {}", g_config.Hostname);

  g_db_client = std::make_unique<MariadbClient>();
  if (!g_db_client->Init()) {
    SLURMX_ERROR("Error: Db client init");
    std::exit(1);
  }

  g_db_client->SetUserAndPwd(g_config.DbUser, g_config.DbPassword);
  if (!g_db_client->Connect()) {
    SLURMX_ERROR("Error: Db client connect");
    std::exit(1);
  }

  g_mongodb_client = std::make_unique<MongodbClient>();
  if (!g_mongodb_client) {
    SLURMX_ERROR("Error: MongoDb client Init failed");
    std::exit(1);
  }
  if (!g_mongodb_client->Connect()) {
    SLURMX_ERROR("Error: MongoDb client connect fail");
    std::exit(1);
  }
  g_mongodb_client->Init();

  g_meta_container = std::make_unique<XdNodeMetaContainerSimpleImpl>();
  g_meta_container->InitFromConfig(g_config);

  g_node_keeper = std::make_unique<XdNodeKeeper>();
  std::list<XdNodeAddrAndId> addr_and_id_list;
  for (auto& kv : g_config.Nodes) {
    XdNodeAddrAndId addr_and_id;
    addr_and_id.node_addr = kv.first;
    if (g_meta_container->GetNodeId(kv.first, &addr_and_id.node_id))
      addr_and_id_list.emplace_back(std::move(addr_and_id));
    else {
      SLURMX_TRACE(
          "Node {} doesn't belong to any partition. It will not be "
          "registered.",
          kv.first);
    }
  }
  g_node_keeper->RegisterXdNodes(std::move(addr_and_id_list));

  g_task_scheduler =
      std::make_unique<TaskScheduler>(std::make_unique<MinLoadFirst>());
}

void DestroyCtlXdGlobalVariables() {
  using namespace CtlXd;
  g_node_keeper.reset();
}

int StartServer() {
  // Create log directory recursively.
  try {
    std::filesystem::path log_path{g_config.SlurmCtlXdLogFile};
    auto log_dir = log_path.parent_path();
    if (!log_dir.empty()) std::filesystem::create_directories(log_dir);
  } catch (const std::exception& e) {
    SLURMX_ERROR("Invalid SlurmXdLogFile path {}: {}",
                 g_config.SlurmCtlXdLogFile, e.what());
  }

  InitializeCtlXdGlobalVariables();

  g_ctlxd_server = std::make_unique<CtlXd::CtlXdServer>(g_config.ListenConf);

  g_ctlxd_server->Wait();

  DestroyCtlXdGlobalVariables();

  return 0;
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
// Todo: Add single program instance checking. The current program will freeze
//  when multiple instances are running.
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif

  if (std::filesystem::exists(kDefaultConfigPath)) {
    try {
      YAML::Node config = YAML::LoadFile(kDefaultConfigPath);

      if (config["SlurmCtlXdListenAddr"])
        g_config.ListenConf.SlurmCtlXdListenAddr =
            config["SlurmCtlXdListenAddr"].as<std::string>();
      else
        g_config.ListenConf.SlurmCtlXdListenAddr = "0.0.0.0";

      if (config["SlurmCtlXdListenPort"])
        g_config.ListenConf.SlurmCtlXdListenPort =
            config["SlurmCtlXdListenPort"].as<std::string>();
      else
        g_config.ListenConf.SlurmCtlXdListenPort = kCtlXdDefaultPort;

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
            SLURMX_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.KeyContent.empty()) {
            SLURMX_ERROR(
                "UseTls is true, but the file specified by KeyFilePath is "
                "empty");
          }
        } else {
          SLURMX_ERROR("UseTls is true, but KeyFilePath is empty");
          std::exit(1);
        }
      } else {
        g_config.ListenConf.UseTls = false;
      }

      if (config["SlurmCtlXdDebugLevel"])
        g_config.SlurmCtlXdDebugLevel =
            config["SlurmCtlXdDebugLevel"].as<std::string>();
      else
        std::exit(1);

      if (config["SlurmCtlXdLogFile"])
        g_config.SlurmCtlXdLogFile =
            config["SlurmCtlXdLogFile"].as<std::string>();
      else
        g_config.SlurmCtlXdLogFile = "/tmp/slurmctlxd/slurmctlxd.log";

      if (config["DbUser"])
        g_config.DbUser = config["DbUser"].as<std::string>();
      else
        std::exit(1);

      if (config["DbPassword"])
        g_config.DbPassword = config["DbPassword"].as<std::string>();
      else
        std::exit(1);

      if (config["MongodbHost"])
        g_config.MongodbHost = config["MongodbHost"].as<std::string>();
      else
        std::exit(1);

      if (config["MongodbPort"])
        g_config.MongodbPort = config["MongodbPort"].as<std::string>();
      else
        std::exit(1);

      if (config["MongodbUser"])
        g_config.MongodbUser = config["MongodbUser"].as<std::string>();
      else
        std::exit(1);

      if (config["MongodbPassword"])
        g_config.MongodbPassword = config["MongodbPassword"].as<std::string>();
      else
        std::exit(1);

      if (config["SlurmCtlXdForeground"]) {
        auto val = config["SlurmCtlXdForeground"].as<std::string>();
        if (val == "true")
          g_config.SlurmCtlXdForeground = true;
        else
          g_config.SlurmCtlXdForeground = false;
      }

      if (config["Nodes"]) {
        for (auto it = config["Nodes"].begin(); it != config["Nodes"].end();
             ++it) {
          auto node = it->as<YAML::Node>();
          auto node_ptr = std::make_shared<CtlXd::Config::Node>();
          std::list<std::string> name_list;

          if (node["name"]) {
            if (!util::ParseHostList(node["name"].Scalar(), &name_list)) {
              SLURMX_ERROR("Illegal node name string format.");
              std::exit(1);
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
          for (auto&& name : name_list) g_config.Nodes[name] = node_ptr;
        }
      }

      if (config["Partitions"]) {
        for (auto it = config["Partitions"].begin();
             it != config["Partitions"].end(); ++it) {
          auto partition = it->as<YAML::Node>();
          std::string name;
          std::string nodes;
          CtlXd::Config::Partition part;

          if (partition["name"]) {
            name.append(partition["name"].Scalar());
          } else
            std::exit(1);

          if (partition["nodes"]) {
            nodes = partition["nodes"].as<std::string>();
          } else
            std::exit(1);

          part.nodelist_str = nodes;
          std::list<std::string> name_list;
          if (!util::ParseHostList(absl::StripAsciiWhitespace(nodes).data(),
                                   &name_list)) {
            SLURMX_ERROR("Illegal node name string format.");
            std::exit(1);
          }

          for (auto&& node : name_list) {
            auto node_it = g_config.Nodes.find(node);
            if (node_it != g_config.Nodes.end()) {
              node_it->second->partition_name = name;
              part.nodes.emplace(node_it->first);
              SLURMX_TRACE("Set the partition of node {} to {}", node_it->first,
                           name);
            }
          }

          g_config.Partitions.emplace(std::move(name), std::move(part));
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
        ("l,listen", "listening address",
         cxxopts::value<std::string>()->default_value("0.0.0.0"))
        ("p,port", "listening port",
         cxxopts::value<std::string>()->default_value(kCtlXdDefaultPort))
        ;
    // clang-format on

    auto parsed_args = options.parse(argc, argv);

    g_config.ListenConf.SlurmCtlXdListenAddr =
        parsed_args["listen"].as<std::string>();
    g_config.ListenConf.SlurmCtlXdListenPort =
        parsed_args["port"].as<std::string>();
  }

  std::regex regex_addr(
      R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

  if (!std::regex_match(g_config.ListenConf.SlurmCtlXdListenAddr, regex_addr)) {
    fmt::print("Listening address is invalid.\n");
    std::exit(1);
  }

  if (!std::regex_match(g_config.ListenConf.SlurmCtlXdListenPort, regex_port)) {
    fmt::print("Listening port is invalid.\n");
    std::exit(1);
  }

  if (g_config.SlurmCtlXdForeground)
    StartServer();
  else
    StartDaemon();

  return 0;
}