cmake_minimum_required(VERSION 3.16)
project(SlurmX)

set(CMAKE_CXX_STANDARD 17)

#set(CMAKE_VERBOSE_MAKEFILE ON)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Check LTO support
    include(CheckIPOSupported)
    check_ipo_supported(RESULT supported OUTPUT error)
    if (supported)
        message(STATUS "IPO / LTO enabled")
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    else ()
        message(STATUS "IPO / LTO not supported: <${error}>")
    endif ()

    # static link on c++ libs
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")

elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    message(STATUS "IPO / LTO disabled due to gcc bugs.")
else ()
    message(STATUS "IPO / LTO disabled for all untested compilers.")
endif ()

# Set colorized output when ninja build system is used.
option(FORCE_COLORED_OUTPUT "Always produce ANSI-colored output (GNU/Clang only)." TRUE)
if (${FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    endif ()
endif ()


# Find third-party libraries
set(DEPENDENCIES_PRE_INSTALLED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/pre_installed)
set(DEPENDENCIES_ONLINE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/online)

find_package(Threads REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.67 REQUIRED COMPONENTS thread fiber)
add_definitions(-DBoost_MINOR_VERSION=${Boost_MINOR_VERSION})

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/dependencies/download/cgroup/lib/libcgroup.a)
    message(FATAL_ERROR "Can not find libcgroup.a! Run dependencies/download_cgroup.sh to download and compile static libcgroup")
endif()

# concurrent queue has no cmake config files. Add it manually as a header-only library.
add_library(concurrentqueue INTERFACE)
target_include_directories(concurrentqueue INTERFACE ${DEPENDENCIES_PRE_INSTALLED_DIR}/concurrentqueue/include)


# Enable SubDirList macro
include(${CMAKE_SOURCE_DIR}/CMakeModule/CMakeMacros.cmake)

add_subdirectory(dependencies/cmake)

# Source Code
add_subdirectory(src)

# Tests
add_subdirectory(test)

# Generate proto files
set(GENERATED_FILE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_FILE_PATH}/protos)
message(${GENERATED_FILE_PATH})

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/generated/*")

## This generates proto files of slurmx.proto file
get_filename_component(slurmx_proto "protos/slurmx.proto" ABSOLUTE)
get_filename_component(slurmx_proto_path "${slurmx_proto}" PATH)
# Generated sources
set(slurmx_proto_srcs "${GENERATED_FILE_PATH}/protos/slurmx.pb.cc")
set(slurmx_proto_hdrs "${GENERATED_FILE_PATH}/protos/slurmx.pb.h")
set(slurmx_grpc_srcs "${GENERATED_FILE_PATH}/protos/slurmx.grpc.pb.cc")
set(slurmx_grpc_hdrs "${GENERATED_FILE_PATH}/protos/slurmx.grpc.pb.h")
add_custom_command(
        OUTPUT "${slurmx_proto_srcs}" "${slurmx_proto_hdrs}" "${slurmx_grpc_srcs}" "${slurmx_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${GENERATED_FILE_PATH}/protos"
        --cpp_out "${GENERATED_FILE_PATH}/protos"
        -I "${slurmx_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${slurmx_proto}"
        DEPENDS "${slurmx_proto}")

# This generates proto files of PublicDefs.proto file
get_filename_component(PublicDefs_proto "protos/PublicDefs.proto" ABSOLUTE)
get_filename_component(PublicDefs_proto_path "${PublicDefs_proto}" PATH)
# Generated sources
set(PublicDefs_proto_srcs "${GENERATED_FILE_PATH}/protos/PublicDefs.pb.cc")
set(PublicDefs_proto_hdrs "${GENERATED_FILE_PATH}/protos/PublicDefs.pb.h")
set(PublicDefs_grpc_srcs "${GENERATED_FILE_PATH}/protos/PublicDefs.grpc.pb.cc")
set(PublicDefs_grpc_hdrs "${GENERATED_FILE_PATH}/protos/PublicDefs.grpc.pb.h")
add_custom_command(
        OUTPUT "${PublicDefs_proto_srcs}" "${PublicDefs_proto_hdrs}" "${PublicDefs_grpc_srcs}" "${PublicDefs_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${GENERATED_FILE_PATH}/protos"
        --cpp_out "${GENERATED_FILE_PATH}/protos"
        -I "${PublicDefs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PublicDefs_proto}"
        DEPENDS "${PublicDefs_proto}")

#proto
add_library(proto
        ${slurmx_grpc_srcs}
        ${slurmx_grpc_hdrs}
        ${slurmx_proto_srcs}
        ${slurmx_proto_hdrs}
        ${PublicDefs_grpc_srcs}
        ${PublicDefs_grpc_hdrs}
        ${PublicDefs_proto_srcs}
        ${PublicDefs_proto_hdrs})

target_link_libraries(proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        absl::base
        absl::synchronization)

target_include_directories(proto PUBLIC ${GENERATED_FILE_PATH})
