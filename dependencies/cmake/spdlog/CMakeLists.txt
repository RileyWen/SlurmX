include(FetchContent)
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
FetchContent_Declare(
        #v1.8.5
        spdlog
        URL https://github.com/gabime/spdlog/archive/100f30043f33277122e0991c83845a2617172ffd.zip
)
#100f30043f33277122e0991c83845a2617172ffd is the Git commit hash of the spdlog version to use
FetchContent_GetProperties(spdlog)
if (NOT spdlog_POPULATED)
    FetchContent_Populate(spdlog)
    add_subdirectory(${spdlog_SOURCE_DIR} ${spdlog_BINARY_DIR} EXCLUDE_FROM_ALL)
    add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
endif ()