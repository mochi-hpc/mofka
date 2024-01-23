#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <mofka/Client.hpp>
#include <mofka/ServiceHandle.hpp>
#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id")
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t")


PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the Mofka client library";

    py::class_<mofka::Client>(m, "Client")
        .def(py::init<py_margo_instance_id>(), "mid"_a)
        .def("connect",
             [](const mofka::Client& client, const std::string& ssgfile) -> mofka::ServiceHandle {
                return client.connect(mofka::SSGFileName{ssgfile});
             },
            "filename"_a)
        .def("connect",
             [](const mofka::Client& client, uint64_t ssgid) -> mofka::ServiceHandle {
                return client.connect(mofka::SSGGroupID{ssgid});
             },
            "gid"_a)
    ;

    py::class_<mofka::ServiceHandle>(m, "ServiceHandle")
        .def_property_readonly("num_servers", &mofka::ServiceHandle::numServers)
        .def("create_topic",
             [](mofka::ServiceHandle& service, const std::string& name) -> void {
                service.createTopic(name);
             },
             "topic_name"_a)
        .def("add_partition",
            [](mofka::ServiceHandle& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& partition_type,
               const std::string& partition_config,
               const mofka::ServiceHandle::PartitionDependencies& dependencies,
               const std::string& pool_name) {
                service.addPartition(
                    topic_name, server_rank, partition_type,
                    mofka::Metadata{partition_config},
                    dependencies, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "partition_type"_a="memory",
            "partition_config"_a="{}", "dependencies"_a=mofka::ServiceHandle::PartitionDependencies{},
            "pool_name"_a="")
    ;
}

