/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_IMPL_H
#define MOFKA_PRODUCER_IMPL_H

#include "PimplUtil.hpp"
#include "MofkaTopicHandle.hpp"
#include "MofkaPartitionInfo.hpp"
#include "MofkaProducerBatch.hpp"

#include "mofka/TopicHandle.hpp"
#include "mofka/Producer.hpp"
#include "mofka/UUID.hpp"
#include "mofka/Ordering.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class MofkaTopicHandle;

class MofkaProducer : public ProducerInterface {

    public:

    tl::engine           m_engine;
    std::string          m_name;
    BatchSize            m_batch_size;
    ThreadPool           m_thread_pool;
    Ordering             m_ordering;
    SP<MofkaTopicHandle> m_topic;

    tl::remote_procedure m_producer_send_batch;

    std::unordered_map<
        size_t,
        SP<ActiveProducerBatchQueue>> m_batch_queues;
    thallium::mutex                   m_batch_queues_mtx;
    thallium::condition_variable      m_batch_queues_cv;

    size_t                       m_num_posted_ults = 0;
    thallium::mutex              m_num_posted_ults_mtx;
    thallium::condition_variable m_num_posted_ults_cv;

    std::atomic<size_t> m_num_pushed_events = 0;
    std::atomic<size_t> m_num_ready_events = 0;

    MofkaProducer(tl::engine engine,
                 std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 Ordering ordering,
                 SP<MofkaTopicHandle> topic)
    : m_engine{std::move(engine)}
    , m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_ordering(ordering)
    , m_topic(std::move(topic))
    , m_producer_send_batch(m_engine.define("mofka_producer_send_batch"))
    {}

    ~MofkaProducer() {
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

    std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const;
};

}

#endif
