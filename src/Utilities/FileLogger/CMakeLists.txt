file(GLOB_RECURSE FILE_LOGGER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)

add_library(Utility_FileLogger ${FILE_LOGGER_SRC})
target_include_directories(Utility_FileLogger PUBLIC include)
target_link_libraries(Utility_FileLogger PUBLIC spdlog::spdlog)
