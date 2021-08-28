file(GLOB_RECURSE UTILITY_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_executable(utility_test ${UTILITY_TEST_SRC}
        ${CMAKE_SOURCE_DIR}/src/SlurmXd/TaskManager.cpp
        )
target_link_libraries(utility_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        Threads::Threads
        event_core
        cxxopts

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        proto

        test_proto
        shared_test_impl_lib
        )
