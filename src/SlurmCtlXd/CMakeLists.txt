add_executable(slurmctlxd
        CtlXdPublicDefs.h
        CtlXdGrpcServer.h
        CtlXdGrpcServer.cpp
        DbClient.h
        DbClient.cpp
        TaskScheduler.h
        TaskScheduler.cpp
        XdNodeKeeper.h
        XdNodeKeeper.cpp
        XdNodeMetaContainer.h
        XdNodeMetaContainer.cpp
        SlurmCtlXd.cpp
        )

target_link_libraries(slurmctlxd
        spdlog::spdlog
        concurrentqueue

        Utility_AnonymousPipe
        Utility_cgroup
        Utility_PublicHeader
        Utility_TimerSet

        Boost::boost
        Boost::thread

        libevent::core
        libevent::pthreads

        cxxopts::cxxopts
        Threads::Threads

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        proto

        yaml-cpp
        mariadbclient
        )

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(slurmctlxd stdc++fs)
endif ()
