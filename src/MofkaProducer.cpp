/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Producer.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Future.hpp>

#include "Result.hpp"
#include "Promise.hpp"
#include "MofkaProducer.hpp"
#include "ActiveProducerBatchQueue.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <limits>

namespace mofka {

MofkaProducer::MofkaProducer(
        tl::engine engine,
        std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        diaspora::Ordering ordering,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        std::shared_ptr<MofkaTopicHandle> topic)
: BatchProducer(name, batch_size, max_batch, ordering, std::move(thread_pool), topic)
, m_engine{std::move(engine)}
, m_mofka_topic(std::move(topic))
, m_producer_send_batch(m_engine.define("mofka_producer_send_batch"))
{}

std::shared_ptr<ProducerBatchInterface> MofkaProducer::newBatchForPartition(size_t index) const {
    if(index >= m_mofka_topic->m_partitions.size()) {
        throw diaspora::Exception{"Invalid index returned by partition selector"};
    }
    auto partition = m_mofka_topic->m_partitions[index];
    return std::make_shared<MofkaProducerBatch>(
            m_name,
            m_engine,
            m_mofka_topic->m_serializer,
            partition->m_ph,
            m_producer_send_batch
    );
}

}
