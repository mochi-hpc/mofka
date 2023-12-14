/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "RapidJsonUtil.hpp"
#include "mofka/Consumer.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include "CerealArchiveAdaptor.hpp"
#include "EventImpl.hpp"
#include "Promise.hpp"
#include "ClientImpl.hpp"
#include "ConsumerImpl.hpp"
#include "PimplUtil.hpp"
#include "ThreadPoolImpl.hpp"
#include "ConsumerBatchImpl.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/vector.hpp>

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
        future = std::move(self->m_futures.front().second);
        self->m_futures.pop_front();
        self->m_futures_credit = false;
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
        m_thread_pool->pushWork(
            [this, i](){
                pullFrom(i, m_pulling_ult_completed[i]);
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

void ConsumerImpl::pullFrom(size_t target_info_index,
                            thallium::eventual<void>& ev) {
    auto& target = m_targets[target_info_index];
    auto& rpc = m_topic->m_service->m_client->m_consumer_request_events;
    auto& ph = target.self->m_ph;
    auto consumer_ctx = reinterpret_cast<intptr_t>(this);
    Result<void> result =
        rpc.on(ph)(consumer_ctx,
                   target_info_index,
                   m_uuid,
                   m_name,
                   0, 0);
    // TODO use max_item, batch_size (and some more options)
    ev.set_value();
}

void ConsumerImpl::recvBatch(size_t target_info_index,
                             size_t count,
                             EventID startID,
                             const BulkRef &metadata_sizes,
                             const BulkRef &metadata,
                             const BulkRef &data_desc_sizes,
                             const BulkRef &data_desc) {

    auto& target = m_targets[target_info_index];

    auto batch = std::make_shared<ConsumerBatchImpl>(
        m_engine, count, metadata.size, data_desc.size);
    batch->pullFrom(metadata_sizes, metadata, data_desc_sizes, data_desc);

    auto serializer = m_topic->m_serializer;
    thallium::future<void> ults_completed{(uint32_t)count};
    size_t metadata_offset  = 0;
    size_t data_desc_offset = 0;

    for(size_t i = 0; i < count; ++i) {
        auto eventID = startID + i;
        // get a promise/future pair
        Promise<Event> promise;
        {
            std::unique_lock<thallium::mutex> guard{m_futures_mtx};
            if(!m_futures_credit || m_futures.empty()) {
                // the queue of futures is empty or the futures
                // already in the queue have been created by
                // previous calls to recvBatch() that haven't had
                // a corresponding pull() call from the user.
                Future<Event> future;
                std::tie(future, promise) = Promise<Event>::CreateFutureAndPromise();
                m_futures.emplace_back(promise, future);
                m_futures_credit = false;
            } else {
                // the queue of futures has futures already
                // created by pull() calls from the user
                promise = std::move(m_futures.front().first);
                m_futures.pop_front();
                m_futures_credit = true;
            }
        }
        // create new event instance
        auto event_impl = std::make_shared<EventImpl>(
            eventID, target.self, shared_from_this());
        // create the ULT
        auto ult = [this, &batch, i, event_impl, promise,
                    metadata_offset, data_desc_offset,
                    &serializer, &ults_completed]() mutable {
            try {
                // deserialize its metadata
                Metadata metadata{event_impl->m_metadata};
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
                // request Data associated with the event
                event_impl->m_data = requestData(
                        event_impl->m_target,
                        event_impl->m_metadata,
                        descriptor.self);
                // set the promise
                promise.setValue(Event{event_impl});
            } catch(const Exception& ex) {
                // something bad happened somewhere,
                // pass the exception to the promise.
                promise.setException(ex);
            }
            ults_completed.set(nullptr);
        };
        m_thread_pool->pushWork(std::move(ult), eventID);
        metadata_offset  += batch->m_meta_sizes[i];
        data_desc_offset += batch->m_data_desc_sizes[i];
    }
    ults_completed.wait();
}

SP<DataImpl> ConsumerImpl::requestData(
        SP<PartitionTargetInfoImpl> target,
        SP<MetadataImpl> metadata,
        SP<DataDescriptorImpl> descriptor) {
    // run data selector
    auto requested_descriptor = (m_data_selector && m_data_broker)
        ? m_data_selector(metadata, descriptor)
        : DataDescriptor::Null();
    if(requested_descriptor.size() == 0)
        return Data{}.self;
    // run data broker
    auto data = m_data_broker(metadata, requested_descriptor);
    if(data.size() != requested_descriptor.size()) {
        throw Exception(
                "DataBroker returned a Data object with a "
                "size different from the DataDescriptor size");
    }
    // expose the local_data_target for RDMA
    std::vector<std::pair<void*, size_t>> segments;
    segments.reserve(data.segments().size());
    for(auto& s : data.segments()) {
        segments.emplace_back((void*)s.ptr, s.size);
    }
    auto local_bulk_ref = BulkRef{
        m_engine.expose(segments, thallium::bulk_mode::write_only),
            0, data.size(),
            m_self_addr
    };
    // request data
    auto& rpc = m_topic->m_service->m_client->m_consumer_request_data;
    auto& ph  = target->m_ph;

    Result<std::vector<Result<void>>> result = rpc.on(ph)(
            Cerealized<DataDescriptor>(descriptor),
            local_bulk_ref);

    if(!result.success())
        throw Exception(result.error());

    if(!result.value()[0].success()) {
        throw Exception(result.value()[0].error());
    }

    return data.self;
}

}
