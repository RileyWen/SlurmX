include(FetchContent)
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
FetchContent_Declare(
        #release-2.1.12-stable
        libevent
        URL https://github.com/libevent/libevent/archive/5df3037d10556bfcb675bc73e516978b75fc7bc7.zip
)
#5df3037d10556bfcb675bc73e516978b75fc7bc7 is the Git commit hash of the libevent version to use
FetchContent_GetProperties(libevent)
if (NOT libevent_POPULATED)
    FetchContent_Populate(libevent)
    # Link libevent as a static lib
    set(LIBEVENT_STATIC_LINK TRUE  CACHE STRING " ")
    add_subdirectory(${libevent_SOURCE_DIR} ${libevent_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()