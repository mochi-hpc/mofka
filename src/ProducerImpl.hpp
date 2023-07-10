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
    BatchSize                        m_batch_size;
    ThreadPool                       m_thread_pool;
    std::shared_ptr<TopicHandleImpl> m_topic;

    ProducerImpl(std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 std::shared_ptr<TopicHandleImpl> topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic)) {}
};

}

#endif
