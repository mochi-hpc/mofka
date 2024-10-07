/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TopicHandle.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"

#include "PimplUtil.hpp"
#include "MofkaTopicHandle.hpp"
#include "MofkaProducer.hpp"
#include "ConsumerImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

Producer MofkaTopicHandle::makeProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering) const {
    return Producer{std::make_shared<MofkaProducer>(
        m_engine, name, batch_size, std::move(thread_pool), ordering,
        const_cast<MofkaTopicHandle*>(this)->shared_from_this())};
}

Consumer MofkaTopicHandle::makeConsumer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        DataBroker data_broker,
        DataSelector data_selector,
        const std::vector<size_t>& targets) const {
    std::vector<SP<MofkaPartitionInfo>> partitions;
    if(targets.empty()) {
        partitions = m_partitions;
    } else {
        partitions.reserve(targets.size());
        for(auto& partition_index : targets) {
            if(partition_index >= m_partitions.size())
                throw Exception{"Invalid partition index passed to TopicHandle::consumer()"};
            partitions.push_back(m_partitions[partition_index]);
        }
    }
    auto consumer = std::make_shared<ConsumerImpl>(
            m_engine, name, batch_size, std::move(thread_pool),
            data_broker, data_selector,
            const_cast<MofkaTopicHandle*>(this)->shared_from_this(),
            std::move(partitions));
    consumer->subscribe();
    return consumer;
}

void MofkaTopicHandle::markAsComplete() const {
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
        throw Exception{
            fmt::format("Could not mark topic as comleted: {}", ex.what())
        };
    }
    for(auto& result : results) {
        if(!result.success()) throw Exception{result.error()};
    }
}

}
