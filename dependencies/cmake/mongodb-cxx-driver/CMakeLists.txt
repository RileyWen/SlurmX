include(FetchContent)

FetchContent_Declare(mongo_c_driver_repo
        GIT_REPOSITORY https://github.com/mongodb/mongo-c-driver
        GIT_TAG 1.21.1)

FetchContent_Declare(mongo_cxx_driver_repo
        GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver
        GIT_TAG r3.7.0-beta1)

message(STATUS "DEPENDENCIES_ONLINE_DIR: ${DEPENDENCIES_ONLINE_DIR}")

FetchContent_GetProperties(mongo_c_driver_repo)
if (NOT mongo_c_driver_repo_POPULATED)
    FetchContent_Populate(mongo_c_driver_repo)
    message(STATUS "Mongodb c driver source dir: ${mongo_c_driver_repo_SOURCE_DIR}")

    if (NOT EXISTS ${mongo_c_driver_repo_SOURCE_DIR}/cmake-build/build.ninja
            OR NOT EXISTS ${mongo_c_driver_repo_SOURCE_DIR}/cmake-build/CMakeCache.txt)
        execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory "${mongo_c_driver_repo_SOURCE_DIR}/cmake-build")
        execute_process(
                COMMAND ${CMAKE_COMMAND} -Wno-dev -G Ninja -DBUILD_TESTING=OFF -DENABLE_UNINSTALL=OFF -DENABLE_HTML_DOCS=OFF -DENABLE_EXAMPLES=OFF -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_ONLINE_DIR}/mongo-c-driver ..
                WORKING_DIRECTORY ${mongo_c_driver_repo_SOURCE_DIR}/cmake-build
        )
    endif ()
    execute_process(
            COMMAND ninja install
            WORKING_DIRECTORY ${mongo_c_driver_repo_SOURCE_DIR}/cmake-build
    )
endif ()


FetchContent_GetProperties(mongo_cxx_driver_repo)
if (NOT mongo_cxx_driver_repo_POPULATED)
    FetchContent_Populate(mongo_cxx_driver_repo)
    message(STATUS "Mongodb cxx driver source dir: ${mongo_cxx_driver_repo_SOURCE_DIR}")

    if (NOT EXISTS ${mongo_cxx_driver_repo_SOURCE_DIR}/cmake-build/build.ninja
            OR NOT EXISTS ${mongo_cxx_driver_repo_SOURCE_DIR}/cmake-build/CMakeCache.txt)
        execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${mongo_cxx_driver_repo_SOURCE_DIR}/cmake-build)
        execute_process(
                COMMAND ${CMAKE_COMMAND} -Wno-dev -G Ninja -DCMAKE_PREFIX_PATH=${DEPENDENCIES_ONLINE_DIR}/mongo-c-driver -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 -DBUILD_SHARED_AND_STATIC_LIBS=ON -DBUILD_SHARED_LIBS_WITH_STATIC_MONGOC=ON -DENABLE_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_ONLINE_DIR}/mongo-cxx-driver ..
                WORKING_DIRECTORY ${mongo_cxx_driver_repo_SOURCE_DIR}/cmake-build
        )
    endif ()
    execute_process(
            COMMAND ninja install
            WORKING_DIRECTORY ${mongo_cxx_driver_repo_SOURCE_DIR}/cmake-build
    )
endif ()

list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/mongo-c-driver")
list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/mongo-cxx-driver")