/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HANDLE_IMPL_H
#define MOFKA_CONSUMER_HANDLE_IMPL_H

#include <thallium.hpp>
#include "mofka/ConsumerHandle.hpp"
#include <queue>

namespace mofka {

class ConsumerHandleImpl {

    public:

    const intptr_t    m_consumer_id;
    const std::string m_consumer_name;
    const size_t      m_max_events;

    size_t m_sent_events = 0;

    ConsumerHandleImpl(intptr_t id, std::string_view name, size_t max)
    : m_consumer_id(id)
    , m_consumer_name(name.data(), name.size())
    , m_max_events(max) {}

};

}

#endif
