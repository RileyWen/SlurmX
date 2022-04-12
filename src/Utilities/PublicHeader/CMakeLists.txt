add_library(Utility_PublicHeader
        String.cpp Network.cpp FdFunctions.cpp PublicHeader.cpp
        include/slurmx/String.h
        include/slurmx/Network.h
        include/slurmx/FdFunctions.h
        include/slurmx/PublicHeader.h
        include/slurmx/BoostInterprocessBarrier.h
        include/slurmx/Lock.h
        include/slurmx/Pointer.h
        )
target_include_directories(Utility_PublicHeader PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} include)
target_link_libraries(Utility_PublicHeader PUBLIC spdlog::spdlog proto)