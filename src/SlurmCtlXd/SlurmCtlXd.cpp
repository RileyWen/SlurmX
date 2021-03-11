#include <grpc++/grpc++.h>

#include <condition_variable>
#include <cxxopts.hpp>
#include <memory>
#include <mutex>
#include <thread>

#include "ConcurrentResourceMgr.h"
#include "CtlXdGrpcServer.h"
#include "PublicHeader.h"

std::mutex g_sigint_mtx;
std::condition_variable g_sigint_cv;
void signal_handler_func(int signum) { g_sigint_cv.notify_one(); };

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

  std::string server_address{fmt::format("{}:{}", address, port)};
  CtlXd::SlurmCtlXdServiceImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server{builder.BuildAndStart()};
  SLURMX_INFO("Server listening on {}", server_address);

  signal(SIGINT, signal_handler_func);

  // Avoid the potential deadlock error in absl::mutex
  std::thread sigint_waiting_thread([p_server = server.get()] {
    std::unique_lock<std::mutex> lk(g_sigint_mtx);
    g_sigint_cv.wait(lk);

    SLURMX_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");
    p_server->Shutdown();
  });
  sigint_waiting_thread.detach();

  server->Wait();

  return 0;
}