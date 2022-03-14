#include <event2/thread.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <yaml-cpp/yaml.h>

#include <condition_variable>
#include <cxxopts.hpp>
#include <filesystem>
#include <mutex>

#include "CtlXdGrpcServer.h"
#include "TaskScheduler.h"
#include "XdNodeKeeper.h"
#include "XdNodeMetaContainer.h"
#include "slurmx/PublicHeader.h"

struct Config {
  std::string address;
  std::string port;
  std::string debug_level;
  std::string log_path;
};

Config g_config;

void InitializeCtlXdGlobalVariables() {
  using namespace CtlXd;

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

  g_node_keeper = std::make_unique<XdNodeKeeper>();
  g_meta_container = std::make_unique<XdNodeMetaContainerSimpleImpl>();

  g_task_scheduler =
      std::make_unique<TaskScheduler>(std::make_unique<MinLoadFirst>());
}

void DestroyCtlXdGlobalVariables() {
  using namespace CtlXd;
  g_node_keeper.reset();
}

int main(int argc, char** argv) {
  // Todo: Add single program instance checking. The current program will freeze
  //  when multiple instances are running.
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif

  if (std::filesystem::exists(kDefaultConfigPath)) {
    try {
      YAML::Node root = YAML::LoadFile(kDefaultConfigPath);

      if (root["SlurmCtlXd"]) {
        YAML::Node config = root["SlurmCtlXd"];
        if (config["listen"])
          g_config.address = config["listen"].as<std::string>();
        else
          g_config.address = "0.0.0.0";

        if (config["port"])
          g_config.port = config["port"].as<std::string>();
        else
          g_config.port = kCtlXdDefaultPort;

        if (config["debug-level"])
          g_config.debug_level = config["debug-level"].as<std::string>();
        else
          return 1;

        if (config["log-path"])
          g_config.log_path = config["log-path"].as<std::string>();
        else
          g_config.log_path = "/tmp/slurmctlxd.log";
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
        ("l,listen", "listening address",
         cxxopts::value<std::string>()->default_value("0.0.0.0"))
        ("p,port", "listening port",
         cxxopts::value<std::string>()->default_value(kCtlXdDefaultPort))
        ;
    // clang-format on

    auto parsed_args = options.parse(argc, argv);

    g_config.address = parsed_args["listen"].as<std::string>();
    g_config.port = parsed_args["port"].as<std::string>();
  }

  std::regex regex_addr(
      R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

  if (!std::regex_match(g_config.address, regex_addr)) {
    fmt::print("Listening address is invalid.\n");
    return 1;
  }

  if (!std::regex_match(g_config.port, regex_port)) {
    fmt::print("Listening port is invalid.\n");
    return 1;
  }

  InitializeCtlXdGlobalVariables();

  std::string server_address{
      fmt::format("{}:{}", g_config.address, g_config.port)};
  g_ctlxd_server = std::make_unique<CtlXd::CtlXdServer>(server_address);

  g_ctlxd_server->Wait();

  DestroyCtlXdGlobalVariables();

  return 0;
}