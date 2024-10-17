# (C) 2020 The University of Chicago
# See COPYRIGHT in top-level directory.
cmake_minimum_required (VERSION 3.21)
project (mofka C CXX)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
enable_testing ()

# library version set here (e.g. for shared libs).
set (MOFKA_VERSION_MAJOR 0)
set (MOFKA_VERSION_MINOR 2)
set (MOFKA_VERSION_PATCH 0)
set (MOFKA_VERSION
     "${MOFKA_VERSION_MAJOR}.${MOFKA_VERSION_MINOR}.${MOFKA_VERSION_PATCH}")

add_library (coverage_config INTERFACE)
add_library (warnings_config INTERFACE)

#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

option (ENABLE_TESTS     "Build tests" OFF)
option (ENABLE_COVERAGE  "Build with coverage" OFF)
option (ENABLE_PYTHON    "Build the Python module" OFF)
option (ENABLE_BENCHMARK "Build the benchmark" OFF)
option (ENABLE_KAFKA     "Build with Kafka support" OFF)

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "ON" CACHE BOOL "Build a shared library")

if (ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options (coverage_config INTERFACE
        -O0        # no optimization
        -g         # generate debug info
        --coverage # sets all required flags
    )
    if (CMAKE_VERSION VERSION_GREATER_EQUAL 3.13)
        target_link_options (coverage_config INTERFACE --coverage)
    else ()
        target_link_libraries (coverage_config INTERFACE --coverage)
    endif ()
endif ()

if (CMAKE_BUILD_TYPE MATCHES "RelWithDebInfo|Debug")
    target_compile_options (warnings_config INTERFACE
        -Wextra -Wall -Wpedantic
    )
endif ()

find_package (PkgConfig REQUIRED)
# search fo thallium
find_package (thallium REQUIRED)
# search for uuid
pkg_check_modules (uuid REQUIRED IMPORTED_TARGET uuid)
# search for nlohmann/json and schema validator
find_package (nlohmann_json REQUIRED)
find_package (nlohmann_json_schema_validator REQUIRED)
# search for spdlog
find_package (spdlog REQUIRED)
# search for fmt package
find_package (fmt REQUIRED)
# search fo bedrock
find_package (bedrock-module-api REQUIRED)
find_package (bedrock REQUIRED)
# search for warabi
find_package (warabi REQUIRED)
# search for yokan
find_package (yokan REQUIRED)
# search for flock
find_package (flock REQUIRED)

if (ENABLE_PYTHON)
    find_package (Python3 COMPONENTS Interpreter Development REQUIRED)
    find_package (pybind11 REQUIRED)
    add_subdirectory (python)
endif ()

if (ENABLE_KAFKA)
    find_package (CURL REQUIRED)
    pkg_check_modules (kafka REQUIRED IMPORTED_TARGET rdkafka)
    set (OPTIONAL_KAFKA CURL::libcurl PkgConfig::kafka)
endif ()

add_subdirectory (src)
add_subdirectory (docs/_code)
add_subdirectory (example)

if (${ENABLE_TESTS})
    enable_testing ()
    find_package (Catch2 QUIET)
    if (NOT Catch2_FOUND)
        include (FetchContent)
        FetchContent_Declare (
            Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG        v3.6.0
        )
        FetchContent_MakeAvailable (Catch2)
    endif ()
    add_subdirectory (tests)
endif (${ENABLE_TESTS})

if (${ENABLE_BENCHMARK})
    add_subdirectory (benchmark)
endif (${ENABLE_BENCHMARK})
