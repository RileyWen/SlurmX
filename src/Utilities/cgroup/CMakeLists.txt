file(GLOB_RECURSE CGROUP_UTILITY_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_library(Utility_cgroup ${CGROUP_UTILITY_SRC})
target_include_directories(Utility_cgroup PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Utility_cgroup PRIVATE PkgConfig::libcgroup spdlog::spdlog Boost::boost)
