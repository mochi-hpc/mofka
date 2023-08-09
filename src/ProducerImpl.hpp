/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_IMPL_H
#define MOFKA_PRODUCER_IMPL_H

#include <thallium.hpp>
#include "TopicHandleImpl.hpp"
#include "PartitionTargetInfoImpl.hpp"
#include "ProducerBatchImpl.hpp"
#include "mofka/Producer.hpp"
#include "mofka/UUID.hpp"
#include <string_view>
#include <queue>

namespace mofka {

class ProducerImpl {

    public:

    std::string                      m_name;
    BatchSize                        m_batch_size;
    ThreadPool                       m_thread_pool;
    std::shared_ptr<TopicHandleImpl> m_topic;

    std::unordered_map<
        PartitionTargetInfo,
        std::shared_ptr<ActiveProducerBatchQueue>> m_batch_queues;
    thallium::mutex                                m_batch_queues_mtx;

    size_t                       m_num_posted_ults = 0;
    thallium::mutex              m_num_posted_ults_mtx;
    thallium::condition_variable m_num_posted_ults_cv;

    size_t m_num_produced_events = 0;

    ProducerImpl(std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 std::shared_ptr<TopicHandleImpl> topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(thread_pool)
    , m_topic(std::move(topic)) {}

};

}

#endif
