add_library(Utility_FileLogger FileLogger.cpp include/slurmx/FileLogger.h)
target_include_directories(Utility_FileLogger PUBLIC include)
target_link_libraries(Utility_FileLogger PUBLIC spdlog::spdlog)
