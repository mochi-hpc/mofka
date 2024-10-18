/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BATCH_PRODUCER_IMPL_H
#define MOFKA_BATCH_PRODUCER_IMPL_H

#include "ProducerBatchInterface.hpp"

#include "mofka/TopicHandle.hpp"
#include "mofka/Producer.hpp"
#include "mofka/Ordering.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class BatchProducer : public ProducerInterface {

    public:

    std::string  m_name;
    BatchSize    m_batch_size;
    ThreadPool   m_thread_pool;
    Ordering     m_ordering;
    TopicHandle  m_topic;

    std::unordered_map<size_t, std::shared_ptr<ActiveProducerBatchQueue>>
                                 m_batch_queues;
    thallium::mutex              m_batch_queues_mtx;
    thallium::condition_variable m_batch_queues_cv;

    size_t                       m_num_posted_ults = 0;
    thallium::mutex              m_num_posted_ults_mtx;
    thallium::condition_variable m_num_posted_ults_cv;

    std::atomic<size_t> m_num_pushed_events = 0;
    std::atomic<size_t> m_num_ready_events = 0;

    BatchProducer(std::string_view name,
                  BatchSize batch_size,
                  ThreadPool thread_pool,
                  Ordering ordering,
                  TopicHandle topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_ordering(ordering)
    , m_topic(std::move(topic))
    {}

    ~BatchProducer() {
        flush();
    }

    const std::string& name() const override {
        return m_name;
    }

    TopicHandle topic() const override;

    BatchSize batchSize() const override {
        return m_batch_size;
    }

    ThreadPool threadPool() const override {
        return m_thread_pool;
    }

    Future<EventID> push(Metadata metadata, Data data) override;

    void flush() override;

    virtual std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const = 0;
};

}

#endif
