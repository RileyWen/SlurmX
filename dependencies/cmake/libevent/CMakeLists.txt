include(FetchContent)
FetchContent_Declare(
        libevent
        GIT_REPOSITORY https://github.com/libevent/libevent
        GIT_TAG release-2.1.12-stable
)

FetchContent_GetProperties(libevent)
if (NOT libevent_POPULATED)
    FetchContent_Populate(libevent)
    # Link libevent as a static lib
    set(LIBEVENT_STATIC_LINK TRUE  CACHE STRING " ")
    add_subdirectory(${libevent_SOURCE_DIR} ${libevent_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()