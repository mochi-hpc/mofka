/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "mofka/Event.hpp"
#include "mofka/Exception.hpp"

#include "MofkaPartitionInfo.hpp"
#include "EventImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Event);

Metadata Event::metadata() const {
    return self->metadata();
}

Data Event::data() const {
    return self->data();
}

PartitionInfo Event::partition() const {
    return self->partition();
}

EventID Event::id() const {
    return self->id();
}

void Event::acknowledge() const {
    self->acknowledge();
}

}
