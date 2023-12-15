/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "RapidJsonUtil.hpp"
#include "mofka/Event.hpp"
#include "mofka/Exception.hpp"

#include "PartitionInfoImpl.hpp"
#include "EventImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Event);

Metadata Event::metadata() const {
    return self->m_metadata;
}

Data Event::data() const {
    return self->m_data;
}

PartitionInfo Event::partition() const {
    return self->m_partition;
}

EventID Event::id() const {
    return self->m_id;
}

void Event::acknowledge() const {
    auto& rpc = self->m_consumer->m_topic->m_service->m_client->m_consumer_ack_event;
    auto& ph  = self->m_partition->m_ph;
    rpc.on(ph)(self->m_consumer->m_name,
               self->m_id);
}

}
