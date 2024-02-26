cmake_minimum_required (VERSION 3.21)
project (mofka-example C CXX)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

if (PROJECT_IS_TOP_LEVEL) # allows building from inside Mofka's source tree
    find_package (mofka REQUIRE)
endif ()

find_package (PkgConfig REQUIRED)
pkg_check_modules (ssg REQUIRED IMPORTED_TARGET ssg)

add_executable (producer producer.cpp)
target_link_libraries (producer mofka::client PkgConfig::ssg)

add_executable (consumer consumer.cpp)
target_link_libraries (consumer mofka::client PkgConfig::ssg)
