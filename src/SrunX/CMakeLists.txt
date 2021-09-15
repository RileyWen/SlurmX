file(GLOB_RECURSE SRUNX-SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(srunx ${SRUNX-SRC})

target_link_libraries(srunx
        mimalloc
        cxxopts
        proto
        Utility_PublicHeader
        )

