include(FetchContent)
FetchContent_Declare(
        libevent
        GIT_REPOSITORY https://github.com/libevent/libevent.git
        GIT_TAG release-2.1.12-stable
)
set(LIBEVENT_STATIC_LINK TRUE)
FetchContent_MakeAvailable(libevent)
