add_library (my_broker_selector  MODULE src/MyDataBrokerDSelector.cpp)
target_link_libraries (my_broker_selector PUBLIC pybind11::module mofka-client PRIVATE coverage_config warnings_config)
pybind11_extension (my_broker_selector)
pybind11_strip (my_broker_selector)

add_library (my_validator  MODULE src/MyValidator.cpp)
target_link_libraries (my_validator  PUBLIC mofka-client PRIVATE coverage_config warnings_config)

add_library (my_serializer  MODULE src/MySerializer.cpp)
target_link_libraries (my_serializer PUBLIC mofka-client PRIVATE coverage_config warnings_config)

add_library (my_partition_selector MODULE src/MyPartitionSelector.cpp)
target_link_libraries (my_partition_selector PUBLIC mofka-client PRIVATE coverage_config warnings_config)

install (TARGETS my_broker_selector my_validator my_serializer my_partition_selector
         ARCHIVE DESTINATION lib/
         LIBRARY DESTINATION lib/)