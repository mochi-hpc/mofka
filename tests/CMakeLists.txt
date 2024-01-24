file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

foreach (test-source ${test-sources})
    get_filename_component (name ${test-source} NAME_WE)
    add_executable (${name} ${test-source})
    target_link_libraries (${name}
        PRIVATE Catch2::Catch2WithMain bedrock-server mofka-client coverage_config warnings_config)
    add_test (NAME ${name} COMMAND ./${name})
    set_property (TEST ${name} PROPERTY ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
endforeach ()

if (ENABLE_PYTHON)
    file (GLOB test-python-sources ${CMAKE_CURRENT_SOURCE_DIR}/python/test-*.py)
    foreach (test-python-source ${test-python-sources})
        get_filename_component (test-target ${test-python-source} NAME_WE)
        set (test-target "${test-target}-python")
        add_test (NAME ${test-target} COMMAND ${Python3_EXECUTABLE} ${test-python-source})
        set_property (TEST ${test-target} PROPERTY ENVIRONMENT
                      LD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}/../src/:$ENV{LD_LIBRARY_PATH})
    endforeach ()
endif ()
