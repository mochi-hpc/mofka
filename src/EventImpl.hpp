/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_IMPL_H
#define MOFKA_EVENT_IMPL_H

#include "PimplUtil.hpp"
#include "ConsumerImpl.hpp"

#include "mofka/Event.hpp"

#include <thallium.hpp>

namespace mofka {

class EventImpl {

    public:

    EventImpl(EventID id,
              SP<PartitionTargetInfoImpl> target,
              SP<ConsumerImpl> consumer)
    : m_id(std::move(id))
    , m_target(std::move(target))
    , m_consumer(std::move(consumer)) {}

    EventID                     m_id;
    SP<PartitionTargetInfoImpl> m_target;
    SP<ConsumerImpl>            m_consumer;
    SP<MetadataImpl>            m_metadata;
    SP<DataImpl>                m_data;
};

}

#endif
