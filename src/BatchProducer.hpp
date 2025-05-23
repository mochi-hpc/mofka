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
    MaxBatch     m_max_batch;
    ThreadPool   m_thread_pool;
    Ordering     m_ordering;
    TopicHandle  m_topic;

    std::vector<std::shared_ptr<ActiveProducerBatchQueue>>
                                 m_batch_queues;
    thallium::mutex              m_batch_queues_mtx;
    thallium::condition_variable m_batch_queues_cv;

    std::atomic<size_t> m_num_pushed_events = 0;

    BatchProducer(std::string_view name,
                  BatchSize batch_size,
                  MaxBatch max_batch,
                  ThreadPool thread_pool,
                  Ordering ordering,
                  TopicHandle topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_max_batch(max_batch)
    , m_thread_pool(std::move(thread_pool))
    , m_ordering(ordering)
    , m_topic(std::move(topic))
    {
        m_batch_queues.resize(m_topic.partitions().size());
    }

    virtual ~BatchProducer() = default;

    const std::string& name() const override {
        return m_name;
    }

    TopicHandle topic() const override;

    BatchSize batchSize() const override {
        return m_batch_size;
    }

    MaxBatch maxBatch() const override {
        return m_max_batch;
    }

    ThreadPool threadPool() const override {
        return m_thread_pool;
    }

    Future<EventID> push(Metadata metadata, Data data, std::optional<size_t> partition) override;

    void flush() override;

    virtual std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const = 0;
};

}

#endif
