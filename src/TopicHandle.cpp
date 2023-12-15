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
    return std::make_shared<ConsumerImpl>(
            self->m_service->m_client->m_engine,
            name, batch_size, thread_pool.self,
            data_broker, data_selector, targets, self);
}

const std::vector<PartitionInfo>& TopicHandle::partitions() const {
    return self->m_partitions;
}

Ordering TopicHandle::defaultOrdering() {
    spdlog::warn("Ordering not specified when creating Producer. "
                 "Ordering will be strict by default. If this was intended, "
                 "explicitely specify the ordering as Ordering::Strict.");
    return Ordering::Strict;
}

}
