file(GLOB_RECURSE TIMER_SET_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp ${CMAKE_CURRENT_SOURCE_DIR}/include/slurmx/*.h)

add_library(Utility_TimerSet ${TIMER_SET_SRC})
target_include_directories(Utility_TimerSet PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} include)
target_link_libraries(Utility_TimerSet PUBLIC
        libevent::core
        libevent::pthreads

        concurrentqueue

        Utility_PublicHeader
        )
