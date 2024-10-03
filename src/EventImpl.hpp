/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_IMPL_H
#define MOFKA_EVENT_IMPL_H

#include "PimplUtil.hpp"
#include "ConsumerImpl.hpp"
#include "MetadataImpl.hpp"
#include "DataImpl.hpp"
#include "MofkaPartitionInfo.hpp"

#include "mofka/Event.hpp"

#include <thallium.hpp>

namespace mofka {

class EventImpl {

    public:

    EventImpl(EventID id,
              SP<MofkaPartitionInfo> partition,
              SP<ConsumerImpl> consumer)
    : m_id(std::move(id))
    , m_partition(std::move(partition))
    , m_consumer(std::move(consumer))
    , m_metadata(std::make_shared<MetadataImpl>("{}", false))
    , m_data(std::make_shared<DataImpl>()) {}

    EventID                m_id;
    SP<MofkaPartitionInfo> m_partition;
    WP<ConsumerImpl>       m_consumer;
    SP<MetadataImpl>       m_metadata;
    SP<DataImpl>           m_data;
};

}

#endif
