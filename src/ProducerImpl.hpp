/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_IMPL_H
#define MOFKA_PRODUCER_IMPL_H

#include "PimplUtil.hpp"
#include "TopicHandleImpl.hpp"
#include "MofkaPartitionInfo.hpp"
#include "ProducerBatchImpl.hpp"

#include "mofka/Producer.hpp"
#include "mofka/UUID.hpp"
#include "mofka/Ordering.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

class ProducerImpl {

    public:

    std::string         m_name;
    BatchSize           m_batch_size;
    SP<ThreadPoolImpl>  m_thread_pool;
    Ordering            m_ordering;
    SP<TopicHandleImpl> m_topic;

    std::unordered_map<
        SP<MofkaPartitionInfo>,
        SP<ActiveProducerBatchQueue>> m_batch_queues;
    thallium::mutex                   m_batch_queues_mtx;
    thallium::condition_variable      m_batch_queues_cv;

    size_t                       m_num_posted_ults = 0;
    thallium::mutex              m_num_posted_ults_mtx;
    thallium::condition_variable m_num_posted_ults_cv;

    std::atomic<size_t> m_num_pushed_events = 0;
    std::atomic<size_t> m_num_ready_events = 0;

    ProducerImpl(std::string_view name,
                 BatchSize batch_size,
                 SP<ThreadPoolImpl> thread_pool,
                 Ordering ordering,
                 SP<TopicHandleImpl> topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_ordering(ordering)
    , m_topic(std::move(topic)) {}

};

}

#endif
