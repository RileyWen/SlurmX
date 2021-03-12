#include "../src/SrunX/SrunXClient.h"
#include "gtest/gtest.h"

// command line parse means tests
TEST(SrunX, OptHelpMessage) {
  int argc = 2;
  const char* argv[] = {"./srunX", "--help"};
  OptParse parser;
  OptParse::AllocatableResource allocatableResource;
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)), SlurmxErr::kOptHelp);
}

TEST(SrunX, OptTest_C_true) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);

  EXPECT_EQ(allocatableResource.cpu_core_limit, (uint64_t)atoi(argv[10]));
}

TEST(SrunX, OptTest_C_Zero) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "0",     "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseZeroErr);
}

TEST(SrunX, OptTest_C_negative) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "-1",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_decimal) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "0.5",   "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_errortype1) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "2m",    "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_errortype2) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "m",     "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};
  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_C_errortype3) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "0M1",   "-m",
                        "200M",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("cpu input:{}", argv[10]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseFailed);
}

TEST(SrunX, OptTest_Memory_range) {
  int argc = 18;
  const char* argv[] = {"./srunX",
                        "-s",
                        "0.0.0.0",
                        "-p",
                        "50051",
                        "-S",
                        "0.0.0.0",
                        "-P",
                        "50052",
                        "-c",
                        "10",
                        "-m",
                        "18446744073709551615m",
                        "-w",
                        "102G",
                        "task",
                        "arg1",
                        "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseRangeErr);
}

TEST(SrunX, OptTest_Memory_true_k) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "128",     "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);
  EXPECT_EQ(allocatableResource.memory_limit_bytes, (uint64_t)131072);
}

TEST(SrunX, OptTest_Memory_true_m) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "128m",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);
  EXPECT_EQ(allocatableResource.memory_limit_bytes, (uint64_t)134217728);
}

TEST(SrunX, OptTest_Memory_true_g) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "128g",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  OptParse::AllocatableResource allocatableResource;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetAllocatableResource(allocatableResource);
  EXPECT_EQ(allocatableResource.memory_limit_bytes, (uint64_t)137438953472);
}

TEST(SrunX, OptTest_Memory_zero) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "0",       "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseZeroErr);
}

TEST(SrunX, OptTest_Memory_errortype1) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "m12",     "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Memory_errortype2) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "2.5m",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[2]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}
TEST(SrunX, OptTest_Memory_errortype4) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "128mm",   "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}
TEST(SrunX, OptTest_Memory_errortype5) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "128y",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;

  SLURMX_INFO("memory input:{}", argv[12]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_true) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",   "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",   "10",    "-m",
                        "128m",    "-w", "102G",    "task", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  parser.Parse(argc, const_cast<char**>(argv));
  parser.GetTaskInfo(task);
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(task.executive_path, argv[15]);
}

TEST(SrunX, OptTest_Task_errortype1) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",    "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",    "10",    "-m",
                        "128m",    "-w", "102G",    "task.", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype2) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",    "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",    "10",    "-m",
                        "128m",    "-w", "102G",    "task-", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype3) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",    "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",    "10",    "-m",
                        "128m",    "-w", "102G",    "task/", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype4) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",     "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",     "10",    "-m",
                        "128m",    "-w", "102G",    "task\\", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype5) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",    "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",    "10",    "-m",
                        "128m",    "-w", "102G",    "task|", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}

TEST(SrunX, OptTest_Task_errortype6) {
  int argc = 18;
  const char* argv[] = {"./srunX", "-s", "0.0.0.0", "-p",    "50051", "-S",
                        "0.0.0.0", "-P", "50052",   "-c",    "10",    "-m",
                        "128m",    "-w", "102G",    "task*", "arg1",  "arg2"};

  OptParse parser;
  OptParse::TaskInfo task;
  SLURMX_INFO("task input:{}", argv[15]);
  ASSERT_EQ(parser.Parse(argc, const_cast<char**>(argv)),
            SlurmxErr::kOptParseTypeErr);
}