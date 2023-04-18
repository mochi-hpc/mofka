add_executable (ClientTest ClientTest.cpp)
target_link_libraries (ClientTest PRIVATE Catch2::Catch2WithMain bedrock-server mofka-client)
add_test (NAME ClientTest COMMAND ./ClientTest)
set_property (TEST ClientTest PROPERTY ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
