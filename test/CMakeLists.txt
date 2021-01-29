file(GLOB_RECURSE TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_executable(slurmx_test
        ${CMAKE_SOURCE_DIR}/src/SlurmXd/TaskManager.cpp
        ${TEST_SRC})
target_link_libraries(slurmx_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        spdlog::spdlog
        Utility_cgroup
        Utility_AnonymousPipe
        Threads::Threads
        libevent::core
        proto
        test_proto
        )
add_test(NAME slurm_test COMMAND test/slurmx_test)

set(PROTO_SRC_DIR ${CMAKE_SOURCE_DIR}/test/protos)
file(GLOB_RECURSE PROTO_SRCS ${PROTO_SRC_DIR}/*.proto)

add_library(test_proto ${PROTO_SRCS})
target_link_libraries(test_proto
        protobuf::libprotobuf
        gRPC::grpc
        gRPC::grpc++)

set(GENERATED_TEST_PROTO_PATH ${CMAKE_SOURCE_DIR}/generated/test)
file(MAKE_DIRECTORY ${GENERATED_TEST_PROTO_PATH})

target_include_directories(test_proto PUBLIC ${GENERATED_TEST_PROTO_PATH})
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_SOURCE_DIR}/generated/test/*")

# This generates the proto part of .proto file
protobuf_generate(
        TARGET test_proto
        LANGUAGE cpp
        PROTOC_OUT_DIR ${GENERATED_TEST_PROTO_PATH}
)

# This generates the grpc part of .proto file
protobuf_generate(
        TARGET test_proto
        LANGUAGE grpc
        PROTOC_OUT_DIR ${GENERATED_TEST_PROTO_PATH}
        PLUGIN "protoc-gen-grpc=${GRPC_CPP_PLUGIN_LOCATION}"
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
)
