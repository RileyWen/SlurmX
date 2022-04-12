# Source files of SlurmCtlXd without 'main' symbol
list(APPEND SLURMCTLXD_SRC_NO_MAIN
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/CtlXdPublicDefs.h
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/CtlXdGrpcServer.h
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/CtlXdGrpcServer.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/DbClient.h
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/DbClient.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/TaskScheduler.h
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/TaskScheduler.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/XdNodeKeeper.h
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/XdNodeKeeper.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/XdNodeMetaContainer.h
        ${PROJECT_SOURCE_DIR}/src/SlurmCtlXd/XdNodeMetaContainer.cpp)

# Source files of SlurmXd without 'main' symbol
list(APPEND SLURMXD_SRC_NO_MAIN
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/CtlXdClient.h
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/CtlXdClient.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/ResourceAllocators.h
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/ResourceAllocators.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/TaskManager.h
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/TaskManager.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/XdServer.h
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/XdServer.cpp
        ${PROJECT_SOURCE_DIR}/src/SlurmXd/XdPublicDefs.h)

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
