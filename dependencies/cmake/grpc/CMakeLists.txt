#Support system-installed gRPC
option(USE_SYSTEM_GRPC "Use system installed gRPC" OFF)
if(USE_SYSTEM_GRPC)
    set(protobuf_MODULE_COMPATIBLE TRUE CACHE STRING " ")
    find_package(Protobuf CONFIG REQUIRED)
    message(STATUS "Using protobuf ${Protobuf_VERSION}")

    set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf CACHE STRING " ")
    set(_REFLECTION gRPC::grpc++_reflection CACHE STRING " ")
    if(CMAKE_CROSSCOMPILING)
        find_program(_PROTOBUF_PROTOC protoc)
    else()
        set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc> CACHE STRING " ")
    endif()

    # Find gRPC installation
    # Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
    find_package(gRPC CONFIG REQUIRED)
    message(STATUS "Using gRPC ${gRPC_VERSION}")

    set(_GRPC_GRPCPP gRPC::grpc++ CACHE STRING " ")
    if(CMAKE_CROSSCOMPILING)
        find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
    else()
        set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin> CACHE STRING " ")
    endif()
else()
    include(FetchContent)
    set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/dependencies/download")
    Set(FETCHCONTENT_QUIET FALSE)
    FetchContent_Declare(
            gRPC
            GIT_REPOSITORY https://github.com/grpc/grpc.git
            GIT_TAG        c5789be22cada39b80d70d4ace3738d62f1c0a04     #v1.28.0
            GIT_SHALLOW TRUE    #work as "git clone --depth 1 --no-single-branch", save time and space
            GIT_PROGRESS TRUE   #see git clone progress info
    )

    set(FETCHCONTENT_QUIET OFF CACHE STRING " ")
    FetchContent_GetProperties(gRPC)
    # The automatically created variables are lowercase.
    string(TOLOWER "gRPC" NAME_LOWER)
    if(NOT ${NAME_LOWER}_POPULATED)
        FetchContent_Populate(${NAME_LOWER})
        add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()
    set(_PROTOBUF_LIBPROTOBUF libprotobuf CACHE STRING " ")
    set(_REFLECTION grpc++_reflection CACHE STRING " ")
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc> CACHE STRING " ")
    set(_GRPC_GRPCPP grpc++ CACHE STRING " ")
    if(CMAKE_CROSSCOMPILING)
        find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
    else()
        set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin> CACHE STRING " ")
    endif()

endif()