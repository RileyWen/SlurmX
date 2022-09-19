cmake_minimum_required(VERSION 3.16)

#set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 17)

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

# The following two variables are used in dependency/cmake files.
set(DEPENDENCIES_PRE_INSTALLED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/pre_installed)
set(DEPENDENCIES_ONLINE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/online)

project(Crane)

find_package(Threads REQUIRED)

# Some content are downloaded and built inside cmake folder.
# This line must be place before any find_package() command.
# Independently built projects are installed to ${DEPENDENCIES_ONLINE_DIR}
#
# Since find_package needs to be in the top scope, we append the paths of installed
# projects at top-level CMakeLists.txt
add_subdirectory(dependencies/cmake)

list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/mongo-c-driver")
list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/mongo-cxx-driver")
find_package(mongocxx REQUIRED)

# Link libevent as a static lib
set(LIBEVENT_STATIC_LINK ON)
list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/libevent")
find_package(Libevent REQUIRED)

# Notify CMake that we have module files to find packages/libs.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModule/")

find_package(PAM REQUIRED)

# Check if we found PAM.
if (NOT PAM_FOUND)
    message(FATAL_ERROR "PAM library was not found.")
endif ()

# Find third-party libraries
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.67 REQUIRED COMPONENTS thread fiber system filesystem)
add_definitions(-DBoost_MINOR_VERSION=${Boost_MINOR_VERSION})

# Needed by grpc
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_GRPCPP grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

find_package(PkgConfig REQUIRED)
pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

# concurrent queue has no cmake config files. Add it manually as a header-only library.
add_library(concurrentqueue INTERFACE)
target_include_directories(concurrentqueue INTERFACE ${DEPENDENCIES_PRE_INSTALLED_DIR}/concurrentqueue/include)

# Proto
add_subdirectory(protos)

# Source Code
add_subdirectory(src)

# Tests
add_subdirectory(test)

# install script
install(TARGETS cranectld craned
        EXPORT ${PROJECT_NAME}Targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)

set(bindir ${CMAKE_INSTALL_PREFIX}/bin)
configure_file(./etc/cranectld.service.in ${PROJECT_BINARY_DIR}/etc/cranectld.service)
configure_file(./etc/craned.service.in ${PROJECT_BINARY_DIR}/etc/craned.service)

install(FILES ${PROJECT_BINARY_DIR}/etc/cranectld.service ${PROJECT_BINARY_DIR}/etc/craned.service DESTINATION /etc/systemd/system/)
install(FILES ./etc/config.yaml.example DESTINATION /etc/crane/)
install(FILES README.md DESTINATION share/doc/crane/)