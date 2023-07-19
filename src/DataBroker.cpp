/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/DataBroker.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include <fmt/format.h>
#include <unordered_map>

namespace mofka {

using DataBrokerImpl = DataBrokerInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(DataBroker);

Data DataBroker::expose(const Metadata& metadata, size_t size) {
    return self->expose(metadata, size);
}

void DataBroker::release(const Metadata& metadata, const Data& data) {
    self->release(metadata, data);
}

}
