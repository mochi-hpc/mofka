/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Producer.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include "Promise.hpp"
#include "KafkaProducer.hpp"
#include "ActiveProducerBatchQueue.hpp"
#include "PimplUtil.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

KafkaProducer::KafkaProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering,
        std::shared_ptr<KafkaTopicHandle> topic,
        std::shared_ptr<rd_kafka_t> kprod,
        std::shared_ptr<rd_kafka_topic_t> ktopic)
: BatchProducer(name, batch_size, std::move(thread_pool), ordering, TopicHandle(topic))
, m_topic(std::move(topic))
, m_kafka_producer(std::move(kprod))
, m_kafka_topic(std::move(ktopic))
{
    start();
}

std::shared_ptr<ProducerBatchInterface> KafkaProducer::newBatchForPartition(size_t index) const {
    if(index >= m_topic->m_partitions.size()) {
        throw Exception{"Invalid index returned by partition selector"};
    }
    return std::make_shared<KafkaProducerBatch>(
            m_kafka_producer, m_kafka_topic, index);
}

void KafkaProducer::start() {
    m_should_stop = false;
    auto run = [this](){
        while(!m_should_stop) {
            int timeout = m_thread_pool.size() > 1 ? 0 : 100;
            rd_kafka_poll(m_kafka_producer.get(), timeout);
            tl::thread::yield();
        }
        m_poll_ult_stopped.set_value();
    };
    m_poll_thread_pool = ThreadPool{ThreadCount{1}};
    m_poll_thread_pool.pushWork(std::move(run));
}

KafkaProducer::~KafkaProducer() {
    if(!m_should_stop) {
        m_should_stop = true;
        m_poll_ult_stopped.wait();
    }
}

}
