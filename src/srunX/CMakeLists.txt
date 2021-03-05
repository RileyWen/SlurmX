file(GLOB_RECURSE SLURMXD_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(srunX ${SLURMXD_SRC})

target_link_libraries(srunX
        cxxopts::cxxopts
        proto
        Utility_PublicHeader
        )

