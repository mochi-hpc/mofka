/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_BATCH_PRODUCER_IMPL_H
#define MOFKA_KAFKA_BATCH_PRODUCER_IMPL_H

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

#include <librdkafka/rdkafka.h>

namespace mofka {

namespace tl = thallium;

class KafkaTopicHandle;

class KafkaBatchProducer : public BatchProducer {

    public:

    std::shared_ptr<KafkaTopicHandle> m_topic;
    std::shared_ptr<rd_kafka_t>       m_kafka_producer;
    std::shared_ptr<rd_kafka_topic_t> m_kafka_topic;
    std::atomic<bool>                 m_should_stop = true;
    tl::eventual<void>                m_poll_ult_stopped;

    mutable size_t                 m_num_pending_batches = 0;
    mutable tl::mutex              m_num_pending_batches_mtx;
    mutable tl::condition_variable m_num_pending_batches_cv;

    KafkaBatchProducer(std::string_view name,
                       BatchSize batch_size,
                       MaxBatch max_batch,
                       ThreadPool thread_pool,
                       Ordering ordering,
                       std::shared_ptr<KafkaTopicHandle> topic,
                       std::shared_ptr<rd_kafka_t> kprod,
                       std::shared_ptr<rd_kafka_topic_t> ktopic);

    ~KafkaBatchProducer();

    void flush() override;

    std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const override;

    void start();
};

}

#endif
