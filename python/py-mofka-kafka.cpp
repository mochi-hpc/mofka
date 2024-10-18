#define PYBIND11_DETAILED_ERROR_MESSAGES
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "pybind11_json/pybind11_json.hpp"
#include <mofka/KafkaDriver.hpp>
#include <mofka/TopicHandle.hpp>

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;


PYBIND11_MODULE(pymofka_kafka, m) {
    m.doc() = "Python binding for the KafkaDriver of the Mofka library";
    py::module_::import("pymofka_client");

    py::class_<mofka::KafkaDriver>(m, "KafkaDriver")
        .def(py::init<const std::string&>(), "config_file"_a)
        .def("create_topic",
             [](mofka::KafkaDriver& service,
                const std::string& name,
                size_t num_partitions,
                size_t replication_factor,
                nlohmann::json config,
                mofka::Validator validator,
                mofka::PartitionSelector selector,
                mofka::Serializer serializer) {
                service.createTopic(
                    name, num_partitions, replication_factor,
                    mofka::Metadata{std::move(config)}, validator, selector, serializer);
             },
             "topic_name"_a, "num_partitions"_a=1, "replication_factor"_a=1,
             "config"_a=nlohmann::json::object(),
             "validator"_a=mofka::Validator{},
             "selector"_a=mofka::PartitionSelector{},
             "serializer"_a=mofka::Serializer{})
        .def("open_topic",
            [](mofka::KafkaDriver& service, const std::string& name) -> mofka::TopicHandle {
                return service.openTopic(name);
            },
            "topic_name"_a)
    ;
}
