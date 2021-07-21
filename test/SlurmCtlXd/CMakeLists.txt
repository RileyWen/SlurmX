file(GLOB_RECURSE SLURMCTLXD_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

# Source files of SlurmCtlXd without 'main' symbol
file(GLOB SLURMCTLXD_SRC
        ${CMAKE_SOURCE_DIR}/src/SlurmCtlXd/*.cpp
        ${CMAKE_SOURCE_DIR}/src/SlurmCtlXd/*.h)
set(SLURMCTLXD_SRC_NO_MAIN ${SLURMCTLXD_SRC})
list(FILTER SLURMCTLXD_SRC_NO_MAIN EXCLUDE REGEX "SlurmCtlXd\\.cpp")

# Source files of SlurmXd without 'main' symbol
file(GLOB SLURMXD_SRC
        ${CMAKE_SOURCE_DIR}/src/SlurmXd/*.cpp
        ${CMAKE_SOURCE_DIR}/src/SlurmXd/*.h)
set(SLURMXD_SRC_NO_MAIN ${SLURMXD_SRC})
list(FILTER SLURMXD_SRC_NO_MAIN EXCLUDE REGEX "SlurmXd\\.cpp")

add_executable(slurmctlxd_test
        ${SLURMCTLXD_SRC_NO_MAIN}
        ${SLURMXD_SRC_NO_MAIN}
        ${SLURMCTLXD_TEST_SRC}
        )
target_include_directories(slurmctlxd_test PUBLIC
        ${CMAKE_SOURCE_DIR}/src/SlurmCtlXd
        ${CMAKE_SOURCE_DIR}/src/SlurmXd
        )

target_link_libraries(slurmctlxd_test
        GTest::gtest
        GTest::gmock
        GTest::gmock_main

        concurrentqueue
        Threads::Threads
        libevent::core
        cxxopts::cxxopts
        Boost::boost
        Boost::thread
        Boost::fiber

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        proto

        test_proto
        shared_test_impl_lib
        )
