include(FetchContent)
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
FetchContent_Declare(
        #v1.42.0
        libuv
        URL https://github.com/libuv/libuv/archive/6ce14710da7079eb248868171f6343bc409ea3a4.zip
)
#6ce14710da7079eb248868171f6343bc409ea3a4 is the Git commit hash of the libuv version to use
FetchContent_GetProperties(libuv)
if (NOT libuv_POPULATED)
    FetchContent_Populate(libuv)
    add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()