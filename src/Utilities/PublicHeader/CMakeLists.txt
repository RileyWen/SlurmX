file(GLOB_RECURSE PUBLIC_HEADER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_library(Utility_PublicHeader ${PUBLIC_HEADER_SRC})
target_include_directories(Utility_PublicHeader PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Utility_PublicHeader PUBLIC spdlog::spdlog proto)