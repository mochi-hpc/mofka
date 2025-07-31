/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Exception.hpp>

#include "Result.hpp"
#include "MofkaTopicHandle.hpp"
#include "MofkaProducer.hpp"
#include "MofkaConsumer.hpp"
#include "MofkaDriver.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

std::shared_ptr<diaspora::ProducerInterface> MofkaTopicHandle::makeProducer(
        std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        diaspora::Ordering ordering,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::Metadata options) {
    (void)options;
    return std::make_shared<MofkaProducer>(
        m_engine, name, batch_size, max_batch, ordering, std::move(thread_pool),
        shared_from_this());
}

std::shared_ptr<diaspora::ConsumerInterface> MofkaTopicHandle::makeConsumer(
        std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector,
        const std::vector<size_t>& targets,
        diaspora::Metadata options) {
    (void)options;
    std::vector<std::shared_ptr<MofkaPartitionInfo>> partitions;
    if(targets.empty()) {
        partitions = m_partitions;
    } else {
        partitions.reserve(targets.size());
        for(auto& partition_index : targets) {
            if(partition_index >= m_partitions.size())
                throw diaspora::Exception{
                    "Invalid partition index passed to TopicHandle::consumer()"};
            partitions.push_back(m_partitions[partition_index]);
        }
    }
    auto consumer = std::make_shared<MofkaConsumer>(
            m_engine, name, batch_size, max_batch,
            std::move(thread_pool), data_allocator, data_selector,
            shared_from_this(),
            std::move(partitions));
    consumer->subscribe();
    return consumer;
}

void MofkaTopicHandle::markAsComplete() {
    auto rpc = m_topic_mark_as_complete;
    std::vector<tl::async_response> responses;
    std::vector<Result<void>> results;
    responses.reserve(m_partitions.size());
    results.reserve(m_partitions.size());
    try {
        for(auto& partition : m_partitions) {
            auto& ph = partition->m_ph;
            responses.push_back(rpc.on(ph).async());
        }
        for(auto& response : responses)
            results.push_back(static_cast<Result<void>>(response.wait()));
    } catch(const std::exception& ex) {
        throw diaspora::Exception{
            fmt::format("Could not mark topic as comleted: {}", ex.what())
        };
    }
    for(auto& result : results) {
        if(!result.success()) throw diaspora::Exception{result.error()};
    }
}

std::shared_ptr<diaspora::DriverInterface> MofkaTopicHandle::driver() const {
    return m_driver;
}

}
