cmake_minimum_required(VERSION 3.17)
project(SlurmX)

set(CMAKE_CXX_STANDARD 17)


# Find third-party libraries
set(THIRD_PARTY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

find_package(fmt 7.1.3 REQUIRED HINTS ${THIRD_PARTY_DIR}/fmt)
find_package(Boost 1.71 REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

LIST(APPEND THIRD_PARTY_LIBS proto fmt::fmt)

file(GLOB_RECURSE SLURMXD_SRC src/SlurmXd/*.cpp src/SlurmXd/*.h)
add_executable(slurmxd ${SLURMXD_SRC})
target_link_libraries(slurmxd
        fmt::fmt
        PkgConfig::libcgroup
        )

file(GLOB_RECURSE CGROUP_UTILITY_SRC src/Utilities/*.cpp src/Utilities/*.h)
add_library(cgroup_utility ${CGROUP_UTILITY_SRC})
target_include_directories(cgroup_utility PUBLIC src/Utilities)
target_link_libraries(cgroup_utility PRIVATE fmt::fmt boost)

# Tests
add_executable(test_server test/grpc_server_test.cpp)
target_link_libraries(test_server ${THIRD_PARTY_LIBS})

add_executable(test_client test/grpc_client_test.cpp)
target_link_libraries(test_client ${THIRD_PARTY_LIBS})

add_executable(test_io_redirect test/io_redirect_test.cpp)
target_link_libraries(test_io_redirect fmt::fmt)


# Generate proto files
find_package(Threads REQUIRED)
find_package(Protobuf CONFIG REQUIRED HINTS ${THIRD_PARTY_DIR}/grpc)
find_package(gRPC CONFIG REQUIRED HINTS ${THIRD_PARTY_DIR}/grpc)

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

set(PROTO_SRC_DIR protos)
file(GLOB_RECURSE PROTO_SRCS ${PROTO_SRC_DIR}/*.proto)

add_library(proto ${PROTO_SRCS})
target_link_libraries(proto
        protobuf::libprotobuf
        gRPC::grpc
        gRPC::grpc++)

set(GENERATED_FILE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_FILE_PATH})

target_include_directories(proto PUBLIC ${GENERATED_FILE_PATH})
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/generated/*")

# This generates the proto part of .proto file
protobuf_generate(
        TARGET proto
        LANGUAGE cpp
        PROTOC_OUT_DIR ${GENERATED_FILE_PATH}
)

# This generates the grpc part of .proto file
protobuf_generate(
        TARGET proto
        LANGUAGE grpc
        PROTOC_OUT_DIR ${GENERATED_FILE_PATH}
        PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}"
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
)