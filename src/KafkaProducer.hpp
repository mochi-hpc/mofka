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
#include "BatchProducer.hpp"

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

class KafkaProducer : public BatchProducer {

    public:

    std::shared_ptr<KafkaTopicHandle> m_topic;
    std::shared_ptr<rd_kafka_t>       m_kafka_producer;
    std::shared_ptr<rd_kafka_topic_t> m_kafka_topic;
    std::atomic<bool>                 m_should_stop = true;
    tl::eventual<void>                m_poll_ult_stopped;
    ThreadPool                        m_poll_thread_pool;

    KafkaProducer(std::string_view name,
                  BatchSize batch_size,
                  ThreadPool thread_pool,
                  Ordering ordering,
                  std::shared_ptr<KafkaTopicHandle> topic,
                  std::shared_ptr<rd_kafka_t> kprod,
                  std::shared_ptr<rd_kafka_topic_t> ktopic);

    ~KafkaProducer();

    std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const override;

    void start();
};

}

#endif
