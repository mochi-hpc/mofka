add_executable (AdminTest AdminTest.cpp)
target_link_libraries (AdminTest PRIVATE Catch2::Catch2WithMain mofka-server mofka-admin)
add_test (NAME AdminTest COMMAND ./AdminTest)

add_executable (ClientTest ClientTest.cpp)
target_link_libraries (ClientTest PRIVATE Catch2::Catch2WithMain mofka-server mofka-client mofka-admin)
add_test (NAME ClientTest COMMAND ./ClientTest)

add_executable (TopicTest TopicTest.cpp)
target_link_libraries (TopicTest PRIVATE Catch2::Catch2WithMain mofka-server mofka-client mofka-admin)
add_test (NAME TopicTest COMMAND ./TopicTest)
