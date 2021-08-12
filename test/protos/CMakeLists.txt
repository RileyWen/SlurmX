# The path used for #include
set(GENERATED_TEST_PATH ${CMAKE_SOURCE_DIR}/generated/test)

# The path where proto cpp/h are generated
set(GENERATED_TEST_PROTO_PATH ${GENERATED_TEST_PATH}/protos)

file(MAKE_DIRECTORY ${GENERATED_TEST_PROTO_PATH})

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_SOURCE_DIR}/generated/test/*")

## This generates proto files of greeter.proto file
get_filename_component(greeter_proto "greeter.proto" ABSOLUTE)
get_filename_component(greeter_proto_path "${greeter_proto}" PATH)

# Generated sources
set(greeter_proto_srcs "${GENERATED_TEST_PROTO_PATH}/greeter.pb.cc")
set(greeter_proto_hdrs "${GENERATED_TEST_PROTO_PATH}/greeter.pb.h")
set(greeter_grpc_srcs "${GENERATED_TEST_PROTO_PATH}/greeter.grpc.pb.cc")
set(greeter_grpc_hdrs "${GENERATED_TEST_PROTO_PATH}/greeter.grpc.pb.h")
add_custom_command(
        OUTPUT "${greeter_proto_srcs}" "${greeter_proto_hdrs}" "${greeter_grpc_srcs}" "${greeter_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${GENERATED_TEST_PROTO_PATH}"
        --cpp_out "${GENERATED_TEST_PROTO_PATH}"
        -I "${greeter_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${greeter_proto}"
        DEPENDS "${greeter_proto}")

# This generates proto files of math.proto file
get_filename_component(math_proto "math.proto" ABSOLUTE)
get_filename_component(math_proto_path "${math_proto}" PATH)

# Generated sources
set(math_proto_srcs "${GENERATED_TEST_PATH}/protos/math.pb.cc")
set(math_proto_hdrs "${GENERATED_TEST_PATH}/protos/math.pb.h")
set(math_grpc_srcs "${GENERATED_TEST_PATH}/protos/math.grpc.pb.cc")
set(math_grpc_hdrs "${GENERATED_TEST_PATH}/protos/math.grpc.pb.h")
add_custom_command(
        OUTPUT "${math_proto_srcs}" "${math_proto_hdrs}" "${math_grpc_srcs}" "${math_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${GENERATED_TEST_PATH}/protos"
        --cpp_out "${GENERATED_TEST_PATH}/protos"
        -I "${math_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${math_proto}"
        DEPENDS "${math_proto}")

#proto
add_library(test_proto
        ${greeter_grpc_srcs}
        ${greeter_grpc_hdrs}
        ${greeter_proto_srcs}
        ${greeter_proto_hdrs}
        ${math_grpc_srcs}
        ${math_grpc_hdrs}
        ${math_proto_srcs}
        ${math_proto_hdrs})

target_link_libraries(test_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        absl::base
        absl::synchronization)

target_include_directories(test_proto PUBLIC ${GENERATED_TEST_PATH})
