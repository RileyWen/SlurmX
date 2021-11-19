file(GLOB_RECURSE SLURMCTLXD_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(slurmctlxd ${SLURMCTLXD_SRC})

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
        )
