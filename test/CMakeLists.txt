file(GLOB_RECURSE TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_executable(slurmx_test ${TEST_SRC})
target_link_libraries(slurmx_test
        GTest::gtest
        GTest::gtest_main
        spdlog::spdlog
        Utility_cgroup
        Utility_AnonymousPipe
        Threads::Threads
        libevent::core
        proto)
add_test(NAME slurm_test COMMAND test/slurmx_test)
