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
    auto consumer = self->m_consumer.lock();
    if(!consumer) {
        throw Exception{"Consumer of this Event has disappeared"};
    }
    auto& rpc = consumer->m_topic->m_service->m_client->m_consumer_ack_event;
    auto& ph  = self->m_partition->m_ph;
    rpc.on(ph)(consumer->m_name, self->m_id);
}

}
