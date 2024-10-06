/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TopicHandle.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"

#include "PimplUtil.hpp"
#include "TopicHandleImpl.hpp"
#include "ProducerImpl.hpp"
#include "ConsumerImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(TopicHandle);

const std::string& TopicHandle::name() const {
    return self->m_name;
}

Producer TopicHandle::makeProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering) const {
    return std::make_shared<ProducerImpl>(
        self->m_engine, name, batch_size, std::move(thread_pool), ordering, self);
}

Consumer TopicHandle::makeConsumer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        DataBroker data_broker,
        DataSelector data_selector,
        const std::vector<size_t>& targets) const {
    std::vector<SP<MofkaPartitionInfo>> partitions;
    if(targets.empty()) {
        partitions = self->m_partitions;
    } else {
        partitions.reserve(targets.size());
        for(auto& partition_index : targets) {
            if(partition_index >= self->m_partitions.size())
                throw Exception{"Invalid partition index passed to TopicHandle::consumer()"};
            partitions.push_back(self->m_partitions[partition_index]);
        }
    }
    auto consumer = std::make_shared<ConsumerImpl>(
            self->m_service->m_client.engine(),
            name, batch_size, std::move(thread_pool),
            data_broker, data_selector, self,
            std::move(partitions));
    consumer->subscribe();
    return consumer;
}

const std::vector<PartitionInfo>& TopicHandle::partitions() const {
    return self->m_partitions_info;
}

void TopicHandle::markAsComplete() const {
    auto engine = self->m_service->m_client.engine();
    auto rpc = self->m_topic_mark_as_complete;
    std::vector<tl::async_response> responses;
    std::vector<Result<void>> results;
    responses.reserve(self->m_partitions.size());
    results.reserve(self->m_partitions.size());
    try {
        for(auto& partition : self->m_partitions) {
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
