#include <mofka/DataSelector.hpp>
#include <mofka/DataBroker.hpp>
#include <pybind11/pybind11.h>

namespace py = pybind11;
using namespace pybind11::literals;

mofka::DataDescriptor selector(const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
    (void)metadata;
    return descriptor;
}

mofka::Data broker(const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) {
    (void)metadata;
    return mofka::Data{new char[descriptor.size()], descriptor.size()};
}

PYBIND11_MODULE(utils, m) {
    m.doc() = "Data broker, data selector binding";
    m.def("selector", &selector, "Data selector");
    m.def("broker", &broker, "Data broker");
}