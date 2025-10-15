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
set (MOFKA_TEST_TOPIC_ARGS
     "{\"partitions\":1}")

add_test (NAME DiasporaTestSuite COMMAND
          diaspora-run-tests.sh mofka
                                --before ${CMAKE_CURRENT_SOURCE_DIR}/pre-test.sh
                                --after ${CMAKE_CURRENT_SOURCE_DIR}/post-test.sh
                                --backend-args ${MOFKA_TEST_BACKEND_ARGS}
                                --topic-args ${MOFKA_TEST_TOPIC_ARGS})
set_property (TEST DiasporaTestSuite PROPERTY
              ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")

add_test (NAME MofkaBenchmark COMMAND
          ${CMAKE_CURRENT_SOURCE_DIR}/run-benchmark.sh)
set_property (TEST MofkaBenchmark PROPERTY
              ENVIRONMENT
              "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}"
              "PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}")
