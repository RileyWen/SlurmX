file(GLOB_RECURSE UTILITY_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_executable(utility_test ${UTILITY_TEST_SRC})
target_link_libraries(utility_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        Threads::Threads
        libevent::core
        cxxopts::cxxopts
        absl::synchronization

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        Utility_TimerSet
        proto

        test_proto
        shared_test_impl_lib
        )
