include(FetchContent)

set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
FetchContent_Declare(
        #v2.0.2
        mimalloc
        URL https://github.com/microsoft/mimalloc/archive/b3b0fb58326a96beea1f4872bc1489d1d9fda7e0.zip
)
#b3b0fb58326a96beea1f4872bc1489d1d9fda7e0 is the Git commit hash of the mimalloc version to use
FetchContent_GetProperties(mimalloc)
if (NOT mimalloc_POPULATED)
    FetchContent_Populate(mimalloc)
    add_subdirectory(${mimalloc_SOURCE_DIR} ${mimalloc_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()
