file (GLOB test-python-sources ${CMAKE_CURRENT_SOURCE_DIR}/test-*.py)

set (MOFKA_TEST_LD_LIBRARY_PATH
    ${CMAKE_BINARY_DIR}/src:${CMAKE_CURRENT_BINARY_DIR}/customs:$ENV{LD_LIBRARY_PATH})
set (MOFKA_TEST_PYTHONPATH
    ${CMAKE_SOURCE_DIR}/python:${CMAKE_BINARY_DIR}/python:${CMAKE_CURRENT_BINARY_DIR}/customs:$ENV{PYTHONPATH})

foreach (test-python-source ${test-python-sources})
    get_filename_component (test-target ${test-python-source} NAME_WE)
    set (test-target "${test-target}-python")
    add_test (NAME ${test-target} COMMAND ${Python3_EXECUTABLE} ${test-python-source})
    set_property (TEST ${test-target} PROPERTY ENVIRONMENT
                  "LD_LIBRARY_PATH=${MOFKA_TEST_LD_LIBRARY_PATH};PYTHONPATH=${MOFKA_TEST_PYTHONPATH}")
endforeach ()

add_subdirectory (customs)
