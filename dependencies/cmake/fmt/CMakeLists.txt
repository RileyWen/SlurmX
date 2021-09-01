include(FetchContent)
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
FetchContent_Declare(
        #8.0.1
        fmt
        URL https://github.com/fmtlib/fmt/archive/d141cdbeb0fb422a3fb7173b285fd38e0d1772dc.zip
)
#d141cdbeb0fb422a3fb7173b285fd38e0d1772dc is the Git commit hash of the fmt version to use
FetchContent_GetProperties(fmt)
if (NOT fmt_POPULATED)
    FetchContent_Populate(fmt)
    add_subdirectory(${fmt_SOURCE_DIR} ${fmt_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()