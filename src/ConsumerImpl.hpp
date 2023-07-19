/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_IMPL_H
#define MOFKA_CONSUMER_IMPL_H

#include <thallium.hpp>
#include "TopicHandleImpl.hpp"
#include "PartitionTargetInfoImpl.hpp"
#include "BatchImpl.hpp"
#include "mofka/Consumer.hpp"
#include "mofka/UUID.hpp"
#include <string_view>
#include <queue>

namespace mofka {

class ConsumerImpl {

    public:

    std::string                      m_name;
    BatchSize                        m_batch_size;
    ThreadPool                       m_thread_pool;
    DataBroker                       m_data_broker;
    DataSelector                     m_data_selector;
    EventProcessor                   m_event_processor;
    std::shared_ptr<TopicHandleImpl> m_topic;

    ConsumerImpl(std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 DataBroker broker,
                 DataSelector selector,
                 std::shared_ptr<TopicHandleImpl> topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_data_broker(std::move(broker))
    , m_data_selector(std::move(selector))
    , m_topic(std::move(topic)) {}

};

}

#endif
