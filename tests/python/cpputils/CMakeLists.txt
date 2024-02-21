cmake_minimum_required (VERSION 3.8)

project (mofka CXX)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package (mofka REQUIRED)
find_package (Python3 COMPONENTS Interpreter Development REQUIRED)
find_package (pybind11 REQUIRED)

add_library (my_broker_selector  MODULE src/MyDataBrokerDSelector.cpp)
target_link_libraries (my_broker_selector pybind11::module mofka-client)
pybind11_extension (my_broker_selector)
pybind11_strip (my_broker_selector)
set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

add_library (my_validator  MODULE src/MyValidator.cpp)
target_link_libraries (my_validator  mofka-client)

add_library (my_serializer  MODULE src/MySerializer.cpp)
target_link_libraries (my_serializer  mofka-client)

add_library (my_partition_selector MODULE src/MyPartitionSelector.cpp)
target_link_libraries (my_partition_selector  mofka-client)

install (TARGETS my_broker_selector my_validator my_serializer my_partition_selector
         ARCHIVE DESTINATION lib/
         LIBRARY DESTINATION lib/)
