file (GLOB mofka-test-sources ${CMAKE_CURRENT_SOURCE_DIR}/Mofka*Test.cpp)

if (OFF)
foreach (test-source ${mofka-test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain bedrock-server mofka coverage_config warnings_config)
    add_test (NAME ${name} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test-with-mofka.sh ./${name})
    set_property (TEST ${name} PROPERTY
                  ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
endforeach ()
endif ()

set (MOFKA_TEST_BACKEND_ARGS
     "{\"group_file\":\"mofka.json\",\"margo\":{\"use_progress_thread\":true}}")
file (READ mofka-legacy-topic-args.json MOFKA_TEST_LEGACY_TOPIC_ARGS)
file (READ mofka-memory-topic-args.json MOFKA_TEST_MEMORY_TOPIC_ARGS)

execute_process (
    COMMAND diaspora-run-tests.sh --list-binary-tests
    OUTPUT_VARIABLE DIASPORA_BINARY_TESTS
    OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process (
    COMMAND diaspora-run-tests.sh --list-python-tests
    OUTPUT_VARIABLE DIASPORA_PYTHON_TESTS
    OUTPUT_STRIP_TRAILING_WHITESPACE)

if (DIASPORA_BINARY_TESTS)
    string (REPLACE "\n" ";" DIASPORA_BINARY_TESTS "${DIASPORA_BINARY_TESTS}")
endif ()

if (DIASPORA_PYTHON_TESTS)
    string (REPLACE "\n" ";" DIASPORA_PYTHON_TESTS "${DIASPORA_PYTHON_TESTS}")
endif ()

foreach (partition_suffix IN ITEMS Legacy Memory)
    if (partition_suffix STREQUAL "Legacy")
        set (topic_args "${MOFKA_TEST_LEGACY_TOPIC_ARGS}")
    else ()
        set (topic_args "${MOFKA_TEST_MEMORY_TOPIC_ARGS}")
    endif ()

    foreach (test_name IN LISTS DIASPORA_BINARY_TESTS)
        add_test (NAME DiasporaTestSuite-${partition_suffix}-Partition-Binary-${test_name}
                  COMMAND diaspora-run-tests.sh mofka
                          --run-binary ${test_name}
                          --before ${CMAKE_CURRENT_SOURCE_DIR}/pre-test.sh
                          --after ${CMAKE_CURRENT_SOURCE_DIR}/post-test.sh
                          --backend-args ${MOFKA_TEST_BACKEND_ARGS}
                          --topic-args ${topic_args})
        set_property (TEST DiasporaTestSuite-${partition_suffix}-Partition-Binary-${test_name}
                      PROPERTY ENVIRONMENT
                      "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
    endforeach ()

    foreach (test_name IN LISTS DIASPORA_PYTHON_TESTS)
        add_test (NAME DiasporaTestSuite-${partition_suffix}-Partition-Python-${test_name}
                  COMMAND diaspora-run-tests.sh mofka
                          --run-python ${test_name}
                          --before ${CMAKE_CURRENT_SOURCE_DIR}/pre-test.sh
                          --after ${CMAKE_CURRENT_SOURCE_DIR}/post-test.sh
                          --backend-args ${MOFKA_TEST_BACKEND_ARGS}
                          --topic-args ${topic_args})
        set_property (TEST DiasporaTestSuite-${partition_suffix}-Partition-Python-${test_name}
                      PROPERTY ENVIRONMENT
                      "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}"
                      "PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}")
    endforeach ()
endforeach ()

add_test (NAME MofkaBenchmark COMMAND
          ${CMAKE_CURRENT_SOURCE_DIR}/run-benchmark.sh)
set_property (TEST MofkaBenchmark PROPERTY
              ENVIRONMENT
              "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}"
              "PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}")
