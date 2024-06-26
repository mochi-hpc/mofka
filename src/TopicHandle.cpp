/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TopicHandle.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"

#include "PimplUtil.hpp"
#include "ClientImpl.hpp"
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

ServiceHandle TopicHandle::service() const {
    return ServiceHandle(self->m_service);
}

Producer TopicHandle::makeProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering) const {
    return std::make_shared<ProducerImpl>(name, batch_size, thread_pool.self, ordering, self);
}

Consumer TopicHandle::makeConsumer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        DataBroker data_broker,
        DataSelector data_selector,
        const std::vector<PartitionInfo>& targets) const {
    auto consumer = std::make_shared<ConsumerImpl>(
            self->m_service->m_client->m_engine,
            name, batch_size, thread_pool.self,
            data_broker, data_selector, targets, self);
    consumer->subscribe();
    return consumer;
}

const std::vector<PartitionInfo>& TopicHandle::partitions() const {
    return self->m_partitions;
}

void TopicHandle::markAsComplete() const {
    auto engine = self->m_service->m_client->m_engine;
    auto rpc = self->m_service->m_client->m_topic_mark_as_complete;
    std::vector<tl::async_response> responses;
    std::vector<Result<void>> results;
    responses.reserve(self->m_partitions.size());
    results.reserve(self->m_partitions.size());
    try {
        for(auto& partition : self->m_partitions) {
            auto ph = tl::provider_handle{
                engine.lookup(partition.address()),
                    partition.providerID()};
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

Ordering TopicHandle::defaultOrdering() {
//    spdlog::warn("Ordering not specified when creating Producer. "
//                 "Ordering will be strict by default. If this was intended, "
//                 "explicitely specify the ordering as Ordering::Strict.");
    return Ordering::Strict;
}

}
