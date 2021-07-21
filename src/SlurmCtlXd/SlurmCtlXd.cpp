#include <grpc++/grpc++.h>

#include <condition_variable>
#include <cxxopts.hpp>
#include <memory>
#include <mutex>
#include <thread>

#include "CtlXdGrpcServer.h"
#include "PublicHeader.h"
#include "XdNodeKeeper.h"
#include "XdNodeMetaContainer.h"

void InitializeCtlXdGlobalVariables() {
  using namespace CtlXd;
  g_node_keeper = std::make_unique<XdNodeKeeper>();
  g_meta_container = std::make_unique<XdNodeMetaContainerSimpleImpl>();
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

  InitializeCtlXdGlobalVariables();

  std::string server_address{fmt::format("{}:{}", address, port)};
  g_ctlxd_server = std::make_unique<CtlXd::CtlXdServer>(server_address);

  g_ctlxd_server->Wait();

  DestroyCtlXdGlobalVariables();

  return 0;
}