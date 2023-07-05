/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_IMPL_H
#define MOFKA_PRODUCER_IMPL_H

#include "TopicHandleImpl.hpp"
#include "mofka/Producer.hpp"
#include "mofka/UUID.hpp"
#include <string_view>

namespace mofka {

class ProducerImpl {

    public:

    std::string                      m_name;
    ProducerOptions                  m_options;
    std::shared_ptr<TopicHandleImpl> m_topic;

    ProducerImpl(std::string_view name,
                 ProducerOptions options,
                 std::shared_ptr<TopicHandleImpl> topic)
    : m_name(name)
    , m_options(std::move(options))
    , m_topic(std::move(topic)) {}
};

}

#endif
