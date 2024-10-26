/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_PRODUCER_IMPL_H
#define MOFKA_KAFKA_PRODUCER_IMPL_H

#include "KafkaTopicHandle.hpp"
#include "KafkaPartitionInfo.hpp"
#include "KafkaProducerBatch.hpp"

#include "mofka/TopicHandle.hpp"
#include "mofka/Producer.hpp"
#include "mofka/UUID.hpp"
#include "mofka/Ordering.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class KafkaTopicHandle;

class KafkaProducer : public ProducerInterface {

    public:

    std::string                       m_name;
    BatchSize                         m_batch_size;
    ThreadPool                        m_thread_pool;
    Ordering                          m_ordering;
    std::shared_ptr<KafkaTopicHandle> m_topic;
    std::shared_ptr<rd_kafka_t>       m_kafka_producer;
    std::shared_ptr<rd_kafka_topic_t> m_kafka_topic;
    std::atomic<bool>                 m_should_stop = true;
    tl::eventual<void>                m_poll_ult_stopped;

    size_t                 m_num_pending_messages = 0;
    tl::mutex              m_num_pending_messages_mtx;
    tl::condition_variable m_num_pending_messages_cv;

    KafkaProducer(std::string_view name,
                  BatchSize batch_size,
                  ThreadPool thread_pool,
                  Ordering ordering,
                  std::shared_ptr<KafkaTopicHandle> topic,
                  std::shared_ptr<rd_kafka_t> kprod,
                  std::shared_ptr<rd_kafka_topic_t> ktopic);

    ~KafkaProducer();

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

    void start();
};

}

#endif
