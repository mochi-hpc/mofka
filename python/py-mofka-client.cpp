#define PYBIND11_DETAILED_ERROR_MESSAGES
#define PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF

#include <mofka/MofkaDriver.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "pybind11_json/pybind11_json.hpp"

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the MofkaDriver class";

    auto pydiaspora_stream_api = py::module_::import("pydiaspora_stream_api");
    py::object DriverInterface = (py::object)pydiaspora_stream_api.attr("Driver");

    py::class_<mofka::MofkaDriver,
               std::shared_ptr<mofka::MofkaDriver>>(m, "MofkaDriver", DriverInterface)
        .def(py::init([](const nlohmann::json& options) {
            return std::dynamic_pointer_cast<mofka::MofkaDriver>(
                mofka::MofkaDriver::create(diaspora::Metadata{options}));
        }))
        .def_property_readonly("num_servers", &mofka::MofkaDriver::numServers)
        .def("start_progress_thread", &mofka::MofkaDriver::startProgressThread)
        .def("add_default_partition",
            [](mofka::MofkaDriver& service,
               std::string_view topic_name,
               size_t server_rank,
               std::string_view metadata_provider,
               std::string_view data_provider,
               const nlohmann::json& partition_config,
               const std::string& pool_name) {
                service.addDefaultPartition(
                    topic_name, server_rank,
                    metadata_provider,
                    data_provider,
                    diaspora::Metadata{partition_config},
                    pool_name);
            },
            "topic_name"_a, "server_rank"_a,
            "metadata_provider"_a=std::string_view{},
            "data_provider"_a=std::string_view{},
            "partition_config"_a=nlohmann::json::object(),
            "pool_name"_a="")
        .def("add_metadata_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const std::string database_type,
               const nlohmann::json& database_config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                diaspora::Metadata config;
                config.json()["database"] = nlohmann::json::object();
                config.json()["database"]["type"] = database_type;
                config.json()["database"]["config"] = database_config;
                return service.addDefaultMetadataProvider(server_rank, config, dependencies);
            },
            "server_rank"_a, "database_type"_a="map",
            "database_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_data_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const std::string target_type,
               const nlohmann::json& target_config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                diaspora::Metadata config;
                config.json()["target"] = nlohmann::json::object();
                config.json()["target"]["type"] = target_type;
                config.json()["target"]["config"] = target_config;
                return service.addDefaultDataProvider(server_rank, config, dependencies);
            },
            "server_rank"_a, "target_type"_a="memory",
            "target_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_custom_partition",
            [](mofka::MofkaDriver& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& partition_type,
               const nlohmann::json& partition_config,
               const mofka::MofkaDriver::Dependencies& dependencies,
               const std::string& pool_name) {
                service.addCustomPartition(
                    topic_name, server_rank, partition_type,
                    diaspora::Metadata{partition_config},
                    dependencies, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "partition_type"_a="memory",
            "partition_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::MofkaDriver::Dependencies{},
            "pool_name"_a="")
        .def("add_memory_partition",
            [](mofka::MofkaDriver& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& pool_name) {
                service.addMemoryPartition(topic_name, server_rank, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "pool_name"_a="")
        .def("add_default_data_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const nlohmann::json& config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                return service.addDefaultDataProvider(server_rank, diaspora::Metadata{config}, dependencies);
            },
            "server_rank"_a, "config"_a=R"({"target":{"type":"memory","config":{}}})"_json,
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_default_metadata_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const nlohmann::json& config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                return service.addDefaultMetadataProvider(server_rank, diaspora::Metadata{config}, dependencies);
            },
            "server_rank"_a, "config"_a=R"({"database":{"type":"map","config":{}}})"_json,
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
    ;
}
