file(GLOB_RECURSE SLURMXD_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(slurmxd ${SLURMXD_SRC})

target_link_libraries(slurmxd
        PkgConfig::libuv
        spdlog::spdlog
        concurrentqueue
        Utility_AnonymousPipe
        Utility_cgroup
        proto
        )

