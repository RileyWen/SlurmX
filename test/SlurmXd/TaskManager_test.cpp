#include "../../src/SlurmXd/TaskManager.h"

#include <random>

#include "PublicHeader.h"
#include "gtest/gtest.h"

static std::string RandomFileNameStr() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(100000, 999999);

  return std::to_string(dist(mt));
}

TEST(TaskManager, simple) {
  spdlog::set_level(spdlog::level::trace);

  std::string test_prog_path = "/tmp/slurmxd_test_" + RandomFileNameStr();
  std::string prog_text =
      "#include <iostream>\\n"
      "#include <thread>\\n"
      "#include <chrono>\\n"
      "int main() { std::cout<<\"Hello World!\";"
      //"std::this_thread::sleep_for(std::chrono::seconds(1));"
      "return 1;"
      "}";

  std::string cmd;

  cmd = fmt::format(R"(bash -c 'echo -e '"'"'{}'"'" | g++ -xc++ -o {} -)",
                    prog_text, test_prog_path);
  // spdlog::info("Cmd: {}", cmd);
  system(cmd.c_str());

  TaskManager& tm = TaskManager::GetInstance();
  TaskInfo info{"RileyTest", test_prog_path, {}, {.cpu_core_limit = 2}};

  auto future = tm.AddTaskAsync(std::move(info));
  grpc_resp_t resp = future.get();
  SLURMX_TRACE("status: {}, reason: {}", resp.status,
               resp.reason.has_value() ? resp.reason.value() : "");

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2s);

  kill(getpid(), SIGINT);

  tm.Wait();

  SLURMX_TRACE("Exiting test...");
  // Cleanup
  if (remove(test_prog_path.c_str()) != 0)
    SLURMX_ERROR("Error removing test_prog:", strerror(errno));
}