cmake_minimum_required (VERSION 3.21)
project (mofka-example C CXX)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

if (PROJECT_IS_TOP_LEVEL) # allows building from inside Mofka's source tree
    find_package (mofka REQUIRE)
endif ()

find_package (PkgConfig REQUIRED)
pkg_check_modules (ssg REQUIRED IMPORTED_TARGET ssg)

add_executable (myproducer producer.cpp)
target_link_libraries (myproducer mofka::client PkgConfig::ssg)

add_executable (myconsumer consumer.cpp)
target_link_libraries (myconsumer mofka::client PkgConfig::ssg)

# CUSTOM TOPIC OBJECTS

add_library (energy_validator SHARED energy_validator.cpp)
target_link_libraries (energy_validator mofka::client)

add_library (energy_partition_selector SHARED energy_partition_selector.cpp)
target_link_libraries (energy_partition_selector mofka::client)

add_library (energy_serializer SHARED energy_serializer.cpp)
target_link_libraries (energy_serializer mofka::client)

add_executable (energy_topic energy_topic.cpp)
target_link_libraries (energy_topic mofka::client PkgConfig::ssg)

