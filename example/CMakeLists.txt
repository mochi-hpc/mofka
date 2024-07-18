add_executable (producer producer.cpp)
target_link_libraries (producer mofka::client)

add_executable (consumer consumer.cpp)
target_link_libraries (consumer mofka::client)
