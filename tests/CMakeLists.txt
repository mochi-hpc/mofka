file (GLOB mofka-test-sources ${CMAKE_CURRENT_SOURCE_DIR}/Mofka*Test.cpp)

foreach (test-source ${mofka-test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain bedrock-server mofka-client coverage_config warnings_config)
    add_test (NAME ${name} COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run-test-with-mofka.sh ./${name})
    set_property (TEST ${name} PROPERTY
                  ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
endforeach ()
