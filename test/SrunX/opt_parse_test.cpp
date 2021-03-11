#include "../src/SrunX/SrunXClient.h"
#include "gtest/gtest.h"

// command line parse means tests
TEST(SrunX, OptHelpMessage) {
  int argc = 2;
  const char* argv[] = {"./srunX", "--help"};
  OptParse parser;

  EXPECT_EXIT(parser.GetResult(argc, const_cast<char**>(argv)),
              testing::ExitedWithCode(0), "");
}

TEST(SrunX, OptTest_C_true) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-c", "10"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  EXPECT_EQ(
      parser
          .GetAllocatableResource(
                    parser.GetResult(argc, const_cast<char**>(argv)))
          .cpu_core_limit,
      (uint64_t)atoi(argv[2]));
}

TEST(SrunX, OptTest_C_Zero) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-c", "0"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_DEATH(parser.GetAllocatableResource(
                   parser.GetResult(argc, const_cast<char**>(argv))),
               "");
}

TEST(SrunX, OptTest_C_negative) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "-1"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  //  ASSERT_DEATH(parser.parse(argc, const_cast<char**>(argv)),
  //               "");
  ASSERT_DEATH(parser.GetResult(argc, const_cast<char**>(argv)), "");
}

TEST(SrunX, OptTest_C_decimal) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "0.5"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_DEATH(parser.GetResult(argc, const_cast<char**>(argv)), "");
}

TEST(SrunX, OptTest_C_errortype1) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "2m"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_DEATH(parser.GetResult(argc, const_cast<char**>(argv)), "");
}

TEST(SrunX, OptTest_C_errortype2) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "m"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_DEATH(parser.GetResult(argc, const_cast<char**>(argv)), "");
}

TEST(SrunX, OptTest_C_errortype3) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-c", "0M1"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[2]);
  ASSERT_DEATH(parser.GetResult(argc, const_cast<char**>(argv)), "");
}

TEST(SrunX, OptTest_Memory_range) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "18446744073709551615m"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_DEATH(parser.GetAllocatableResource(
                   parser.GetResult(argc, const_cast<char**>(argv))),
               "");
}

TEST(SrunX, OptTest_Memory_true_k) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  EXPECT_EQ(
      parser
          .GetAllocatableResource(
                    parser.GetResult(argc, const_cast<char**>(argv)))
          .memory_limit_bytes,
      (uint64_t)131072);
}

TEST(SrunX, OptTest_Memory_true_m) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128m"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  EXPECT_EQ(
      parser
          .GetAllocatableResource(
                    parser.GetResult(argc, const_cast<char**>(argv)))
          .memory_limit_bytes,
      (uint64_t)134217728);
}

TEST(SrunX, OptTest_Memory_true_g) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "128g"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  EXPECT_EQ(
      parser
          .GetAllocatableResource(
                    parser.GetResult(argc, const_cast<char**>(argv)))
          .memory_limit_bytes,
      (uint64_t)137438953472);
}

TEST(SrunX, OptTest_Memory_zero) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "0"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_DEATH(parser.GetAllocatableResource(
                   parser.GetResult(argc, const_cast<char**>(argv))),
               "");
}

TEST(SrunX, OptTest_Memory_errortype1) {
  int argc = 3;
  const char* argv[] = {"./srunX", "-m", "m12"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_DEATH(parser.GetAllocatableResource(
                   parser.GetResult(argc, const_cast<char**>(argv))),
               "");
}

TEST(SrunX, OptTest_Memory_errortype2) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "2.5m"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_DEATH(
      {
        parser.GetAllocatableResource(
            parser.GetResult(argc, const_cast<char**>(argv)));
      },
      "");
}
TEST(SrunX, OptTest_Memory_errortype4) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "125mm"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_DEATH(
      {
        parser.GetAllocatableResource(
            parser.GetResult(argc, const_cast<char**>(argv)));
      },
      "");
}
TEST(SrunX, OptTest_Memory_errortype5) {
  int argc = 3;

  const char* argv[] = {"./srunX", "-m", "125p"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_DEATH(parser.GetAllocatableResource(
                   parser.GetResult(argc, const_cast<char**>(argv))),
               "");
}

TEST(SrunX, OptTest_Task_true) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task"};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  EXPECT_EQ(
      parser.GetTaskInfo(parser.GetResult(argc, const_cast<char**>(argv)), uuid)
          .executive_path,
      argv[1]);
}

TEST(SrunX, OptTest_Task_errortype1) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task."};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_DEATH(
      parser.GetTaskInfo(
                   parser.GetResult(argc, const_cast<char**>(argv)), uuid),
      "");
}

TEST(SrunX, OptTest_Task_errortype2) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task-"};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_DEATH(
      parser.GetTaskInfo(
                   parser.GetResult(argc, const_cast<char**>(argv)), uuid),
      "");
}

TEST(SrunX, OptTest_Task_errortype3) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task/"};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_DEATH(
      parser.GetTaskInfo(
                   parser.GetResult(argc, const_cast<char**>(argv)), uuid),
      "");
}

TEST(SrunX, OptTest_Task_errortype4) {
  int argc = 3;

  const char* argv[] = {"./srunX", "task\\"};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_DEATH(
      parser.GetTaskInfo(
                   parser.GetResult(argc, const_cast<char**>(argv)), uuid),
      "");
}

TEST(SrunX, OptTest_Task_errortype5) {
  int argc = 3;

  const char* argv[] = {"./srunX", "task|"};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_DEATH(
      parser.GetTaskInfo(
                   parser.GetResult(argc, const_cast<char**>(argv)), uuid),
      "");
}

TEST(SrunX, OptTest_Task_errortype6) {
  int argc = 3;
  const char* argv[] = {"./srunX", "task*"};

  OptParse parser;
  uuid uuid;

  SLURMX_INFO("task input:{}", argv[1]);
  ASSERT_DEATH(
      parser.GetTaskInfo(
                   parser.GetResult(argc, const_cast<char**>(argv)), uuid),
      "");
}