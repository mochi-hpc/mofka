find_package (MPI REQUIRED)

add_executable (mofka-benchmark benchmark.cpp)
target_link_libraries (mofka-benchmark
    MPI::MPI_C mofka-client mofka-server spdlog::spdlog
    nlohmann_json_schema_validator::validator
    coverage_config warnings_config)
