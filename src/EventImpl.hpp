/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_IMPL_H
#define MOFKA_EVENT_IMPL_H

#include "mofka/Event.hpp"

namespace mofka {

class EventImpl {

    public:

    EventImpl(Metadata metadata,
              Data data,
              PartitionTargetInfo target,
              EventID id)
    : m_metadata(std::move(metadata))
    , m_data(std::move(data))
    , m_target(std::move(target))
    , m_id(std::move(id)) {}

    Metadata            m_metadata;
    Data                m_data;
    PartitionTargetInfo m_target;
    EventID             m_id;
};

}

#endif
