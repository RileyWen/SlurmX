#include "../src/SrunX/SrunXClient.h"
#include "gtest/gtest.h"

// command line parse means tests
TEST(SrunX, OptHelpMessage) {
  int argc = 2;
  const char* argv[] = {"./srunX", "--help"};
  OptParse parser;
  OptParse::AllocatableResource allocatableResource;
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)), SlurmxErr::kOk);
}

TEST(SrunX, OptTest_C_true) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-c", "10"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);

  EXPECT_EQ(allocatableResource.cpu_core_limit, (uint64_t)atoi(argv[2]));
}

TEST(SrunX, OptTest_C_Zero) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-c", "0"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseZeroErr);
}

TEST(SrunX, OptTest_C_negative) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "-1"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_decimal) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "0.5"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_errortype1) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "2m"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_errortype2) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "m"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_errortype3) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "0M1"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_Memory_range) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "18446744073709551615m"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseRangeErr);
}

TEST(SrunX, OptTest_Memory_true_k) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);
  EXPECT_EQ(allocatableResource.memory_limit_bytes, (uint64_t)131072);
}

TEST(SrunX, OptTest_Memory_true_m) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128m"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);
  EXPECT_EQ(allocatableResource.memory_limit_bytes, (uint64_t)134217728);
}

TEST(SrunX, OptTest_Memory_true_g) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128g"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);
  EXPECT_EQ(allocatableResource.memory_limit_bytes, (uint64_t)137438953472);
}

TEST(SrunX, OptTest_Memory_zero) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "0"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseZeroErr);
}

TEST(SrunX, OptTest_Memory_errortype1) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "m12"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Memory_errortype2) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "2.5m"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}
TEST(SrunX, OptTest_Memory_errortype4) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "125mm"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}
TEST(SrunX, OptTest_Memory_errortype5) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "125p"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_true) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task"};

  OptParse parser;
  OptParse::TaskInfo task;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetTaskInfo(task);
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(task.executive_path, argv[1]);
}

TEST(SrunX, OptTest_Task_errortype1) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task."};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype2) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task-"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype3) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task/"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype4) {
  int argc = 3;

  const char* argv[] = {"./srunX", "task\\"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype5) {
  int argc = 3;

  const char* argv[] = {"./srunX", "task|"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype6) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task*"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}