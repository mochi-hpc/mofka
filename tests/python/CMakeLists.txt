file (GLOB test-mofka-python-sources ${CMAKE_CURRENT_SOURCE_DIR}/test_mofka_*.py)
file (GLOB test-kafka-python-sources ${CMAKE_CURRENT_SOURCE_DIR}/test_kafka_*.py)

set (MOFKA_TEST_LD_LIBRARY_PATH
    ${CMAKE_BINARY_DIR}/src:${CMAKE_CURRENT_BINARY_DIR}/customs:$ENV{LD_LIBRARY_PATH})
set (MOFKA_TEST_PYTHONPATH
    ${CMAKE_SOURCE_DIR}/python:${CMAKE_BINARY_DIR}/python:${CMAKE_CURRENT_BINARY_DIR}/customs:$ENV{PYTHONPATH})

foreach (test-python-source ${test-mofka-python-sources})
    get_filename_component (test-target ${test-python-source} NAME_WE)
    set (test-target "${test-target}_py")
    if (${ENABLE_COVERAGE})
        add_test (NAME ${test-target} COMMAND coverage run -a ${test-python-source})
    else ()
        add_test (NAME ${test-target} COMMAND ${Python3_EXECUTABLE} ${test-python-source})
    endif ()
    set_property (TEST ${test-target} PROPERTY ENVIRONMENT
                  "LD_LIBRARY_PATH=${MOFKA_TEST_LD_LIBRARY_PATH};PYTHONPATH=${MOFKA_TEST_PYTHONPATH}")
endforeach ()

if (ENABLE_KAFKA)

foreach (test-python-source ${test-kafka-python-sources})
    get_filename_component (test-target ${test-python-source} NAME_WE)
    set (test-target "${test-target}_py")
    if (${ENABLE_COVERAGE})
        add_test (NAME ${test-target} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/../run-test-with-kafka.sh
                  coverage run -a ${test-python-source})
    else ()
        add_test (NAME ${test-target} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/../run-test-with-kafka.sh
                  ${Python3_EXECUTABLE} ${test-python-source})
    endif ()
    set_property (TEST ${test-target} PROPERTY ENVIRONMENT
                  "LD_LIBRARY_PATH=${MOFKA_TEST_LD_LIBRARY_PATH};PYTHONPATH=${MOFKA_TEST_PYTHONPATH}")
endforeach ()

endif ()

add_subdirectory (customs)
