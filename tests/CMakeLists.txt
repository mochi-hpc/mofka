file (GLOB mofka-test-sources ${CMAKE_CURRENT_SOURCE_DIR}/Mofka*Test.cpp)
file (GLOB kafka-test-sources ${CMAKE_CURRENT_SOURCE_DIR}/Kafka*Test.cpp)

foreach (test-source ${mofka-test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain bedrock-server mofka-client coverage_config warnings_config)
    add_test (NAME ${name} COMMAND ./${name})
    set_property (TEST ${name} PROPERTY
                  ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
endforeach ()

if (ENABLE_KAFKA)

foreach (test-source ${kafka-test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain bedrock-server mofka::client mofka::kafka-driver
                coverage_config warnings_config)
    add_test (NAME ${name} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test-with-kafka.sh ./${name})
    set_property (TEST ${name} PROPERTY
                  ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
endforeach ()

endif ()

if (ENABLE_PYTHON)

add_subdirectory (python)

# Set the path to the python directory
set (PYTHON_MODULE_DIR ${CMAKE_SOURCE_DIR}/python)
# Use file(GLOB_RECURSE ...) to find all files matching the test_mofka_*.py pattern
file (GLOB_RECURSE PYTHON_MOFKA_TEST_FILES "${PYTHON_MODULE_DIR}/test_mofka_*.py")
# Use file(GLOB_RECURSE ...) to find all files matching the test_kafka_*.py pattern
file (GLOB_RECURSE PYTHON_MOFKA_TEST_FILES "${PYTHON_MODULE_DIR}/test_kafka_*.py")

foreach (PYTHON_TEST_FILE ${PYTHON_MOFKA_TEST_FILES})
    # Remove the directory part
    file (RELATIVE_PATH PYTHON_TEST_FILE_REL ${PYTHON_MODULE_DIR} ${PYTHON_TEST_FILE})
    # Remove the file extension
    string (REPLACE ".py" "" PYTHON_TEST_FILE_BASE ${PYTHON_TEST_FILE_REL})
    # Replace slashes with dots
    string (REPLACE "/" "." PYTHON_TEST_NAME ${PYTHON_TEST_FILE_BASE})
    # Add the test
    if (${ENABLE_COVERAGE})
        message (STATUS "${PYTHON_TEST_NAME} test will run with code coverage")
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND coverage run -a -m unittest ${PYTHON_TEST_NAME})
    else ()
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND python -m unittest ${PYTHON_TEST_NAME})
    endif ()
    set_property (TEST ${PYTHON_TEST_NAME} PROPERTY ENVIRONMENT
                  PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH})
endforeach ()

if (ENABLE_KAFKA)

foreach (PYTHON_TEST_FILE ${PYTHON_KAFKA_TEST_FILES})
    # Remove the directory part
    file (RELATIVE_PATH PYTHON_TEST_FILE_REL ${PYTHON_MODULE_DIR} ${PYTHON_TEST_FILE})
    # Remove the file extension
    string (REPLACE ".py" "" PYTHON_TEST_FILE_BASE ${PYTHON_TEST_FILE_REL})
    # Replace slashes with dots
    string (REPLACE "/" "." PYTHON_TEST_NAME ${PYTHON_TEST_FILE_BASE})
    # Add the test
    if (${ENABLE_COVERAGE})
        message (STATUS "${PYTHON_TEST_NAME} test will run with code coverage")
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test-with-kafka.sh
                  coverage run -a -m unittest ${PYTHON_TEST_NAME})
    else ()
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test-with-kafka.sh
                  python -m unittest ${PYTHON_TEST_NAME})
    endif ()
    set_property (TEST ${PYTHON_TEST_NAME} PROPERTY ENVIRONMENT
                  PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH})
endforeach ()

endif ()

endif ()
