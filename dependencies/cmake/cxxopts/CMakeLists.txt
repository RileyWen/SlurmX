include(FetchContent)
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
FetchContent_Declare(
        #v2.2.1
        cxxopts
        URL https://github.com/jarro2783/cxxopts/archive/302302b30839505703d37fb82f536c53cf9172fa.zip
)
#302302b30839505703d37fb82f536c53cf9172fa is the Git commit hash of the cxxopts version to use
FetchContent_GetProperties(cxxopts)
if (NOT cxxopts_POPULATED)
    FetchContent_Populate(cxxopts)
    add_subdirectory(${cxxopts_SOURCE_DIR} ${cxxopts_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()