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

static std::string GenerateTestProg(const std::string& prog_text) {
  std::string test_prog_path = "/tmp/slurmxd_test_" + RandomFileNameStr();
  std::string cmd;

  cmd = fmt::format(R"(bash -c 'echo -e '"'"'{}'"'" | g++ -xc++ -o {} -)",
                    prog_text, test_prog_path);
  system(cmd.c_str());

  return test_prog_path;
}

static void RemoveTestProg(const std::string& test_prog_path) {
  // Cleanup
  if (remove(test_prog_path.c_str()) != 0)
    SLURMX_ERROR("Error removing test_prog:", strerror(errno));
}

TEST(TaskManager, NormalExit) {
  spdlog::set_level(spdlog::level::trace);
  std::string prog_text =
      "#include <iostream>\\n"
      "#include <thread>\\n"
      "#include <chrono>\\n"
      "int main() { std::cout<<\"Hello World!\";"
      "return 1;"
      "}";

  std::string test_prog_path = GenerateTestProg(prog_text);

  auto output_callback = [&](std::string&& buf) {
    SLURMX_DEBUG("Output from callback: {}", buf);

    EXPECT_EQ(buf, "Hello World!");
  };

  auto finish_callback = [&](bool is_terminated_by_signal, int value) {
    SLURMX_DEBUG("Task ended. Normal exit: {}. Value: {}",
                 !is_terminated_by_signal, value);

    EXPECT_EQ(is_terminated_by_signal, false);
    EXPECT_EQ(value, 1);
  };

  TaskManager& tm = TaskManager::GetInstance();
  TaskInitInfo info{
      "RileyTest",           test_prog_path,  {},
      {.cpu_core_limit = 2}, output_callback, finish_callback,
  };

  SlurmxErr err = tm.AddTaskAsync(std::move(info));
  SLURMX_TRACE("err value: {}, reason: {}", uint64_t(err), SlurmxErrStr(err));

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2s);

  // Emulate ctrl+C
  kill(getpid(), SIGINT);

  tm.Wait();

  SLURMX_TRACE("Exiting test...");

  RemoveTestProg(test_prog_path);
}

TEST(TaskManager, SigintTermination) {
  spdlog::set_level(spdlog::level::trace);
  std::string prog_text =
      "#include <iostream>\\n"
      "#include <thread>\\n"
      "#include <chrono>\\n"
      "int main() { std::cout<<\"Hello World!\";"
      "std::this_thread::sleep_for(std::chrono::seconds(10));"
      "return 1;"
      "}";

  SlurmxErr err;

  std::string test_prog_path = GenerateTestProg(prog_text);

  auto output_callback = [&](std::string&& buf) {
    SLURMX_DEBUG("Output from callback: {}", buf);

    EXPECT_EQ(buf, "Hello World!");
  };

  auto finish_callback = [&](bool is_terminated_by_signal, int value) {
    SLURMX_DEBUG("Task ended. Normal exit: {}. Value: {}",
                 !is_terminated_by_signal, value);

    // Kill by SIGINT
    EXPECT_EQ(is_terminated_by_signal, true);

    // signum of SIGINT is 2
    EXPECT_EQ(value, 2);
  };

  TaskManager& tm = TaskManager::GetInstance();
  TaskInitInfo info_1{
      "RileyTest",           test_prog_path,  {},
      {.cpu_core_limit = 2}, output_callback, finish_callback,
  };

  err = tm.AddTaskAsync(std::move(info_1));
  EXPECT_EQ(err, SlurmxErr::kOk);

  TaskInitInfo info_2{
      "RileyTest",           test_prog_path,  {},
      {.cpu_core_limit = 2}, output_callback, finish_callback,
  };
  err = tm.AddTaskAsync(std::move(info_2));
  EXPECT_EQ(err, SlurmxErr::kExistingTask);

  TaskInitInfo info_3{
      "RileyTest_2",         test_prog_path,  {},
      {.cpu_core_limit = 2}, output_callback, finish_callback,
  };
  err = tm.AddTaskAsync(std::move(info_3));
  EXPECT_EQ(err, SlurmxErr::kOk);

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2s);

  // Emulate ctrl+C.
  // This call will trigger the SIGINT handler in TaskManager.
  // We expect that all task will be terminated.
  kill(getpid(), SIGINT);

  tm.Wait();

  SLURMX_TRACE("Exiting test...");

  RemoveTestProg(test_prog_path);
}

// Todo: Test TaskManager from grpc.