/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HANDLE_IMPL_H
#define MOFKA_CONSUMER_HANDLE_IMPL_H

#include <thallium.hpp>
#include "mofka/UUID.hpp"
#include "mofka/ConsumerHandle.hpp"
#include "mofka/TopicManager.hpp"
#include <queue>

namespace mofka {

class ConsumerHandleImpl {

    public:

    const intptr_t                      m_consumer_ctx;
    const std::string                   m_consumer_name;
    const size_t                        m_max_events;
    const std::shared_ptr<TopicManager> m_topic_manager;
    const thallium::endpoint            m_consumer_endpoint;
    const thallium::remote_procedure    m_send_batch;
    std::atomic<bool>                   m_should_stop = false;

    size_t m_sent_events = 0;

    ConsumerHandleImpl(
        intptr_t ctx,
        std::string_view name,
        size_t max,
        std::shared_ptr<TopicManager> topic_manager,
        thallium::endpoint endpoint,
        thallium::remote_procedure rpc)
    : m_consumer_ctx(ctx)
    , m_consumer_name(name.data(), name.size())
    , m_max_events(max)
    , m_topic_manager(std::move(topic_manager))
    , m_consumer_endpoint(std::move(endpoint))
    , m_send_batch(std::move(rpc)) {}

    void stop();
};

}

#endif
