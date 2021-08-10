include(FetchContent)
FetchContent_Declare(GTest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0)
FetchContent_MakeAvailable(GTest)
message("get GTest")