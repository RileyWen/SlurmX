cmake_minimum_required(VERSION 3.16)
project(SlurmX)

set(CMAKE_CXX_STANDARD 17)

#set(CMAKE_VERBOSE_MAKEFILE ON)

# Find third-party libraries
set(THIRD_PARTY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

find_package(Threads REQUIRED)
find_package(fmt 7.1.3 REQUIRED HINTS ${THIRD_PARTY_DIR}/fmt)
find_package(Boost 1.71 REQUIRED)
find_package(GTest CONFIG REQUIRED HINTS ${THIRD_PARTY_DIR}/google-test)

find_package(spdlog 1.8.2 REQUIRED HINTS ${THIRD_PARTY_DIR}/spdlog)
add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)

find_package(Libevent REQUIRED HINTS ${THIRD_PARTY_DIR}/libevent)

find_package(cxxopts 2.2.0 REQUIRED HINTS ${THIRD_PARTY_DIR}/cxxopts)

find_package(Protobuf CONFIG REQUIRED HINTS ${THIRD_PARTY_DIR}/grpc)
find_package(gRPC CONFIG REQUIRED HINTS ${THIRD_PARTY_DIR}/grpc)
get_target_property(GRPC_CPP_PLUGIN_LOCATION gRPC::grpc_cpp_plugin LOCATION)

find_package(PkgConfig REQUIRED)

pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

list(APPEND CMAKE_PREFIX_PATH "${THIRD_PARTY_DIR}/libuv")
pkg_check_modules(libuv REQUIRED IMPORTED_TARGET libuv>=1.40.0)

# concurrent queue has no cmake config files. Add it manually as a header-only library.
add_library(concurrentqueue INTERFACE)
target_include_directories(concurrentqueue INTERFACE ${THIRD_PARTY_DIR}/concurrentqueue/include)

LIST(APPEND THIRD_PARTY_LIBS proto fmt::fmt)

# Source Code
add_subdirectory(src)

# Tests
add_subdirectory(test)

# Generate proto files
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
        PLUGIN "protoc-gen-grpc=${GRPC_CPP_PLUGIN_LOCATION}"
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
)