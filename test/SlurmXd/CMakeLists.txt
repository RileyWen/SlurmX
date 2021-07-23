file(GLOB_RECURSE SLURMXD_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_executable(slurmxd_test
        ${CMAKE_SOURCE_DIR}/src/SlurmXd/TaskManager.cpp
        ${SLURMXD_TEST_SRC})
target_link_libraries(slurmxd_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        spdlog::spdlog
        libevent::core
        libevent::pthreads
        cxxopts::cxxopts

        Threads::Threads

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        proto

        test_proto
        shared_test_impl_lib
        )
