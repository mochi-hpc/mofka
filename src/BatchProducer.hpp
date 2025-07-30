/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BATCH_PRODUCER_IMPL_H
#define MOFKA_BATCH_PRODUCER_IMPL_H

#include "ProducerBatchInterface.hpp"
#include "ActiveProducerBatchQueue.hpp"

#include <diaspora/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Ordering.hpp>
#include <diaspora/Driver.hpp>

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class BatchProducer : public diaspora::ProducerInterface {

    public:

    std::string                                     m_name;
    diaspora::BatchSize                             m_batch_size;
    diaspora::MaxNumBatches                         m_max_batch;
    diaspora::Ordering                              m_ordering;
    std::shared_ptr<diaspora::ThreadPoolInterface>  m_thread_pool;
    std::shared_ptr<diaspora::TopicHandleInterface> m_topic;

    std::vector<std::shared_ptr<ActiveProducerBatchQueue>>
                                 m_batch_queues;
    thallium::mutex              m_batch_queues_mtx;
    thallium::condition_variable m_batch_queues_cv;

    std::atomic<size_t> m_num_pushed_events = 0;

    BatchProducer(std::string_view name,
                  diaspora::BatchSize batch_size,
                  diaspora::MaxNumBatches max_batch,
                  diaspora::Ordering ordering,
                  std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                  std::shared_ptr<diaspora::TopicHandleInterface> topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_max_batch(max_batch)
    , m_ordering(ordering)
    , m_thread_pool(thread_pool ? std::move(thread_pool) : topic->driver()->defaultThreadPool())
    , m_topic(std::move(topic))
    {
        m_batch_queues.resize(m_topic->partitions().size());
    }

    virtual ~BatchProducer() = default;

    const std::string& name() const override {
        return m_name;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_batch;
    }

    diaspora::Ordering ordering() const override {
        return m_ordering;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    diaspora::Future<diaspora::EventID> push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            std::optional<size_t> partition) override;

    void flush() override;

    virtual std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const = 0;
};

}

#endif
