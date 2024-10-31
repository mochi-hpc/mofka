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
#include "KafkaBatchProducer.hpp"
#include "ActiveProducerBatchQueue.hpp"
#include "PimplUtil.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

KafkaBatchProducer::KafkaBatchProducer(
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

std::shared_ptr<ProducerBatchInterface> KafkaBatchProducer::newBatchForPartition(size_t index) const {
    if(index >= m_topic->m_partitions.size()) {
        throw Exception{"Invalid index returned by partition selector"};
    }
    return std::make_shared<KafkaProducerBatch>(
            this, m_topic->m_serializer, m_kafka_producer, m_kafka_topic, index);
}

void KafkaBatchProducer::flush() {
    BatchProducer::flush();
}

void KafkaBatchProducer::start() {
    m_should_stop = false;
    auto run = [this](){
        while(!m_should_stop) {
            {
                std::unique_lock<tl::mutex> pending_guard{m_num_pending_batches_mtx};
                m_num_pending_batches_cv.wait(pending_guard,
                        [this](){ return m_should_stop || m_num_pending_batches > 0; });
            }
            while(rd_kafka_poll(m_kafka_producer.get(), 0) != 0) {
                if(m_should_stop) break;
                tl::thread::yield();
            }
            if(m_should_stop) break;
            tl::thread::yield();
        }
        m_poll_ult_stopped.set_value();
    };
    m_thread_pool.pushWork(std::move(run));
}

KafkaBatchProducer::~KafkaBatchProducer() {
    flush();
    if(!m_should_stop) {
        m_should_stop = true;
        m_num_pending_batches_cv.notify_all();
        m_poll_ult_stopped.wait();
    }
}

}
