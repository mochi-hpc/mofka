add_library (my_broker_selector  MODULE MyDataBrokerDSelector.cpp)
target_link_libraries (my_broker_selector PUBLIC pybind11::module mofka-client PRIVATE coverage_config warnings_config)
pybind11_extension (my_broker_selector)
pybind11_strip (my_broker_selector)

add_library (my_validator  MODULE MyValidator.cpp)
target_link_libraries (my_validator  PUBLIC mofka-client PRIVATE coverage_config warnings_config)

add_library (my_serializer  MODULE MySerializer.cpp)
target_link_libraries (my_serializer PUBLIC mofka-client PRIVATE coverage_config warnings_config)

add_library (my_partition_selector MODULE MyPartitionSelector.cpp)
target_link_libraries (my_partition_selector PUBLIC mofka-client PRIVATE coverage_config warnings_config)
