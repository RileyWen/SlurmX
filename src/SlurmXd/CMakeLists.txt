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
        Boost::filesystem

        libevent::core
        libevent::pthreads

        bs_thread_pool

        cxxopts::cxxopts
        Threads::Threads
        nlohmann_json::nlohmann_json

        absl::flat_hash_map
        absl::synchronization

        yaml-cpp
        )

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(slurmxd stdc++fs)
endif ()