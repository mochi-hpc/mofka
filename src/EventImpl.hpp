/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_IMPL_H
#define MOFKA_EVENT_IMPL_H

#include "ConsumerImpl.hpp"
#include "mofka/Event.hpp"
#include <thallium.hpp>

namespace mofka {

class EventImpl {

    public:

    EventImpl(std::shared_ptr<PartitionTargetInfoImpl> target,
              EventID id,
              std::shared_ptr<ConsumerImpl> consumer)
    : m_target(std::move(target))
    , m_id(std::move(id))
    , m_consumer(std::move(consumer)) {}

    EventImpl(Metadata metadata,
              Data data,
              std::shared_ptr<PartitionTargetInfoImpl> target,
              EventID id,
              std::shared_ptr<ConsumerImpl> consumer)
    : m_metadata(std::move(metadata))
    , m_data(std::move(data))
    , m_target(std::move(target))
    , m_id(std::move(id))
    , m_consumer(std::move(consumer)) {}

    Metadata                                 m_metadata;
    Data                                     m_data;
    std::shared_ptr<PartitionTargetInfoImpl> m_target;
    EventID                                  m_id;
    std::shared_ptr<ConsumerImpl>            m_consumer;
};

}

#endif
