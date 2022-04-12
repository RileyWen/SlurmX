add_executable(slurmxd
        CtlXdClient.h
        CtlXdClient.cpp
        ResourceAllocators.h
        ResourceAllocators.cpp
        TaskManager.h
        TaskManager.cpp
        XdServer.h
        XdServer.cpp
        XdPublicDefs.h
        SlurmXd.cpp
        )

target_link_libraries(slurmxd
        spdlog::spdlog
        concurrentqueue

        Utility_AnonymousPipe
        Utility_cgroup
        Utility_FileLogger
        Utility_PublicHeader

        proto
        Boost::boost

        libevent::core
        libevent::pthreads

        cxxopts::cxxopts
        Threads::Threads
        nlohmann_json::nlohmann_json

        absl::flat_hash_map
        absl::synchronization

        yaml-cpp
        )