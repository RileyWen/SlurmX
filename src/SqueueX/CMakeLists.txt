file(GLOB_RECURSE SQUEUEX-SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(squeuex ${SQUEUEX-SRC})

target_link_libraries(squeuex
        cxxopts::cxxopts
        proto
        Utility_PublicHeader
        absl::time
        )