file(GLOB_RECURSE SRUNX-SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(srunX ${SRUNX-SRC})

target_link_libraries(srunX
        cxxopts::cxxopts
        proto
        Utility_PublicHeader
        )

