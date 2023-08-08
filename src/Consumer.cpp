/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Consumer.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include "AsyncRequestImpl.hpp"
#include "Promise.hpp"
#include "ClientImpl.hpp"
#include "ConsumerImpl.hpp"
#include "PimplUtil.hpp"
#include "ThreadPoolImpl.hpp"
#include "ConsumerBatchImpl.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Consumer);

const std::string& Consumer::name() const {
    return self->m_name;
}

TopicHandle Consumer::topic() const {
    return TopicHandle(self->m_topic);
}

BatchSize Consumer::batchSize() const {
    return self->m_batch_size;
}

ThreadPool Consumer::threadPool() const {
    return self->m_thread_pool;
}

DataBroker Consumer::dataBroker() const {
    return self->m_data_broker;
}

DataSelector Consumer::dataSelector() const {
    return self->m_data_selector;
}

Future<Event> Consumer::pull() const {
    Future<Event> future;
    std::unique_lock<thallium::mutex> guard{self->m_futures_mtx};
    if(self->m_futures_credit || self->m_futures.empty()) {
        // the queue of futures is empty or the futures
        // already in the queue have been created by
        // previous calls to pull() that haven't completed
        Promise<Event> promise;
        std::tie(future, promise) = Promise<Event>::CreateFutureAndPromise();
        self->m_futures.emplace_back(std::move(promise), future);
        self->m_futures_credit = true;
    } else {
        // the queue of futures has futures already
        // created by the consumer
        Future<Event> future = std::move(self->m_futures.back().second);
        self->m_futures.pop_back();
    }
    return future;
}

void Consumer::process(EventProcessor processor,
                       ThreadPool threadPool,
                       NumEvents maxEvents) const {

    // TODO
}

void Consumer::operator|(EventProcessor processor) const && {
    process(processor, self->m_thread_pool, NumEvents::Infinity());
}

NumEvents NumEvents::Infinity() {
    return NumEvents{std::numeric_limits<size_t>::max()};
}

void ConsumerImpl::start() {
    // for each target, submit a ULT that pulls from that target
    auto n = m_targets.size();
    m_pulling_ult_completed.resize(n);
    for(size_t i=0; i < n; ++i) {
        m_thread_pool.self->pushWork(
            [this, i](){
                pullFrom(m_targets[i], m_pulling_ult_completed[i]);
        });
    }
}

void ConsumerImpl::join() {
    // send a message to all the targets requesting to disconnect
    auto& rpc = m_topic->m_service->m_client->m_consumer_remove_consumer;
    for(auto& target : m_targets) {
        auto& ph = target.self->m_ph;
        rpc.on(ph)(m_uuid);
    }
    // wait for the ULTs to complete
    for(auto& ev : m_pulling_ult_completed)
        ev.wait();
}

void ConsumerImpl::pullFrom(const PartitionTargetInfo& target,
                            thallium::eventual<void>& ev) {
    auto& rpc = m_topic->m_service->m_client->m_consumer_request_events;
    auto& ph = target.self->m_ph;
    auto consumer_ctx = reinterpret_cast<intptr_t>(this);
    RequestResult<void> result =
        rpc.on(ph)(m_topic->m_name,
                   consumer_ctx,
                   m_uuid,
                   m_name,
                   0, 0);
    // TODO use max_item, batch_size (and some more options)
    ev.set_value();
}

void ConsumerImpl::recvBatch(size_t count,
                             const BulkRef &metadata_sizes,
                             const BulkRef &metadata,
                             const BulkRef &data_desc_sizes,
                             const BulkRef &data_desc) {
    auto batch = std::make_shared<ConsumerBatchImpl>(
        m_engine, count, metadata.size, data_desc.size);
    batch->pullFrom(metadata_sizes, metadata, data_desc_sizes, data_desc);

    auto serializer = m_topic->m_serializer;
    thallium::future<void> ults_completed{(uint32_t)count};
    size_t metadata_offset  = 0;
    size_t data_desc_offset = 0;

    for(size_t i = 0; i < count; ++i) {
        auto ult = [&batch, i, metadata_offset, data_desc_offset, &serializer, &ults_completed]() {
            // deserialize the metadata
            Metadata metadata;
            BufferWrapperInputArchive metadata_archive{
                std::string_view{
                    batch->m_meta_buffer.data() + metadata_offset,
                    batch->m_meta_sizes[i]}};
            serializer.deserialize(metadata_archive, metadata);
            // deserialize the data descriptors
            BufferWrapperInputArchive descriptors_archive{
                std::string_view{
                    batch->m_data_desc_buffer.data() + data_desc_offset,
                    batch->m_data_desc_sizes[i]}};
            DataDescriptor descriptor;
            descriptor.load(descriptors_archive);
            // TODO
            ults_completed.set(nullptr);
        };
        m_thread_pool.self->pushWork(std::move(ult));
        metadata_offset  += batch->m_meta_sizes[i];
        data_desc_offset += batch->m_data_desc_sizes[i];
    }
    ults_completed.wait();
}

}
