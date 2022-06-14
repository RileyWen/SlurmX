add_library(pam_slurmx SHARED
        Pam.cpp)

# remove "lib" prefix
set_target_properties(pam_slurmx PROPERTIES PREFIX "")

target_include_directories(pam_slurmx PUBLIC ${PAM_INCLUDE_DIR})

target_link_libraries(pam_slurmx PUBLIC
        ${PAM_LIBRARIES}
        proto
        Boost::boost
        spdlog::spdlog
        Utility_PublicHeader
        )

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(pam_slurmx PUBLIC stdc++fs)
endif ()
