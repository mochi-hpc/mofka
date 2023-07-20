/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Event.hpp"
#include "mofka/Exception.hpp"

#include "EventImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Event);

const Metadata& Event::metadata() const {
    return self->m_metadata;
}

const Data& Event::data() const {
    return self->m_data;
}

PartitionTargetInfo Event::partition() const {
    return self->m_target;
}

EventID Event::id() const {
    return self->m_id;
}

void Event::acknowledge() const {
    // TODO
}

}
