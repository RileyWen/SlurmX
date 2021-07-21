file(GLOB_RECURSE SHARED_TEST_IMPL_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h
        )

add_library(shared_test_impl_lib ${SHARED_TEST_IMPL_SRC})

target_link_libraries(shared_test_impl_lib PUBLIC test_proto Utility_PublicHeader)
target_include_directories(shared_test_impl_lib PUBLIC include)

