cmake_minimum_required (VERSION 3.8)

project (mofka CXX)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package (mofka REQUIRED)
find_package (Python3 COMPONENTS Interpreter Development REQUIRED)
find_package (pybind11 REQUIRED)

add_library (utils  MODULE src/test_data_broker_selector.cpp)
target_link_libraries (utils pybind11::module mofka-client)

pybind11_extension (utils)
pybind11_strip (utils)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})
install (TARGETS utils
         ARCHIVE DESTINATION lib/
         LIBRARY DESTINATION lib/)
