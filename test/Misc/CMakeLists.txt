file(GLOB_RECURSE MISC_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_executable(misc_test
        ${MISC_TEST_SRC}
        )

target_link_libraries(misc_test
        GTest::gtest
        GTest::gmock
        GTest::gmock_main

        shared_test_impl_lib

        Utility_PublicHeader
        Utility_AnonymousPipe

        concurrentqueue
        absl::btree
        libevent::core
        libevent::pthreads

        Threads::Threads
        )
