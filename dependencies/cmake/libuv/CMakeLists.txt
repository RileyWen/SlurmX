include(FetchContent)
FetchContent_Declare(
        libuv
        GIT_REPOSITORY https://github.com/libuv/libuv
        GIT_TAG v1.42.0)

FetchContent_GetProperties(libuv)
if (NOT libuv_POPULATED)
    FetchContent_Populate(libuv)
    add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()