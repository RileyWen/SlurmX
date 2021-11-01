file(GLOB_RECURSE SLURMXD_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(slurmxd ${SLURMXD_SRC})

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
        )