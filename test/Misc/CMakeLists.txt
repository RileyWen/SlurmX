add_executable(misc_test
        grpc_example_test.cpp
        libevent_example_test.cpp
        libuv_test.cpp
        mariadb_connector_c_test.cpp
        Misc_test.cpp
        yaml_cpp_test.cpp
        )

target_include_directories(misc_test PRIVATE ${CMAKE_SOURCE_DIR}/src/SlurmCtlXd)

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

        PkgConfig::libuv-static

        Threads::Threads

        mariadbclient

        yaml-cpp
        )
