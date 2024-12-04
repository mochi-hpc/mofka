/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Consumer.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"
#include "mofka/BufferWrapperArchive.hpp"

#include "JsonUtil.hpp"
#include "CerealArchiveAdaptor.hpp"
#include "MofkaEvent.hpp"
#include "Promise.hpp"
#include "MofkaConsumer.hpp"
#include "ConsumerBatchImpl.hpp"
#include <limits>

using namespace std::string_literals;

namespace mofka {

TopicHandle MofkaConsumer::topic() const {
    return TopicHandle{m_topic};
}

Future<Event> MofkaConsumer::pull() {
    Future<Event> future;
    std::unique_lock<thallium::mutex> guard{m_futures_mtx};
    if(m_futures_credit || m_futures.empty()) {
        // the queue of futures is empty or the futures
        // already in the queue have been created by
        // previous calls to pull() that haven't completed
        Promise<Event> promise;
        std::tie(future, promise) = Promise<Event>::CreateFutureAndPromise();
        if(m_completed_partitions != m_partitions.size()) {
            // there are uncompleted partitions, put the future in the queue
            // and it will be picked up by a recvBatch RPC from any partition
            m_futures.emplace_back(std::move(promise), future);
            m_futures_credit = true;
        } else {
            // all partitions are completed, create a NoMoreEvents event
            // (arbitrarily from partition 0)
            // create new event instance
            promise.setValue(Event{std::make_shared<MofkaEvent>()});
        }
    } else {
        // the queue of futures has futures already
        // created by the consumer
        future = std::move(m_futures.front().second);
        m_futures.pop_front();
        m_futures_credit = false;
    }
    return future;
}

void MofkaConsumer::subscribe() {
    // for each partition, send a subscription RPC to that partition
    auto n = m_partitions.size();
    std::vector<thallium::eventual<void>> ult_completed(n);
    for(size_t i=0; i < n; ++i) {
        m_thread_pool.pushWork(
            [this, i, &ult_completed](){
                auto& partition = m_partitions[i];
                auto& rpc = m_consumer_request_events;
                auto& ph = partition->m_ph;
                auto consumer_ptr = reinterpret_cast<intptr_t>(this);
                Result<void> result = rpc.on(ph)(
                    consumer_ptr, (size_t)i, m_name,
                    (size_t)0, m_batch_size.value);
                ult_completed[i].set_value();
            }
        );
    }
    auto mid = m_engine.get_margo_instance();
    margo_set_progress_when_needed(mid, false);
    // wait for the ULTs to complete
    for(auto& ev : ult_completed)
        ev.wait();
}

void MofkaConsumer::unsubscribe() {
    // send a message to all the partitions requesting to disconnect
    auto& rpc = m_consumer_remove_consumer;
    auto n = m_partitions.size();
    std::vector<thallium::eventual<void>> ult_completed(n);
    for(size_t i=0; i < n; ++i) {
        m_thread_pool.pushWork(
            [this, i, &ult_completed, &rpc](){
                auto& partition = m_partitions[i];
                auto& ph = partition->m_ph;
                auto consumer_ptr = reinterpret_cast<intptr_t>(this);
                Result<void> result = rpc.on(ph)(consumer_ptr, i);
                ult_completed[i].set_value();
            }
        );
    }
    // wait for the ULTs to complete
    for(auto& ev : ult_completed)
        ev.wait();
    auto mid = m_engine.get_margo_instance();
    margo_set_progress_when_needed(mid, true);
}

void MofkaConsumer::partitionCompleted() {
    m_completed_partitions += 1;
    // check if there will be no more events any more, if so, set
    // the promise of all the pending Futures to NoMoreEvents.
    std::unique_lock<thallium::mutex> guard{m_futures_mtx};
    if(m_completed_partitions != m_partitions.size())
        return;
    if(!m_futures_credit)
        return;
    while(!m_futures.empty()) {
        auto promise = std::move(m_futures.front().first);
        m_futures.pop_front();
        m_futures_credit = true;
        promise.setValue(Event{std::make_shared<MofkaEvent>()});
    }
}

void MofkaConsumer::recvBatch(const tl::request& req,
                              size_t partition_index,
                              size_t count,
                              EventID startID,
                              const BulkRef &metadata_sizes,
                              const BulkRef &metadata,
                              const BulkRef &data_desc_sizes,
                              const BulkRef &data_desc) {

    auto batch = std::make_shared<ConsumerBatchImpl>(
        m_engine, count, metadata.size, data_desc.size);
    batch->pullFrom(metadata_sizes, metadata, data_desc_sizes, data_desc);

    std::vector<Promise<Event>> promises;
    promises.reserve(count);

    // Create all the Future/Promise pairs first (to avoid locking over and over)
    {
        std::unique_lock<thallium::mutex> guard{m_futures_mtx};
        for(size_t i = 0; i < count; ++i) {
            // get a promise/future pair
            Promise<Event> promise;
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
            promises.push_back(std::move(promise));
        }
    }

    Result<void> result;
    req.respond(result);

    auto ult = [this, self = shared_from_this(),
                count, startID, batch = std::move(batch),
                serializer = m_topic->m_serializer,
                partition = m_partitions[partition_index],
                promises = std::move(promises)]() mutable {

        size_t metadata_offset  = 0;
        size_t data_desc_offset = 0;

        // Deserialize each event
        for(size_t i = 0; i < count; ++i) {
            auto eventID = startID + i;
            try {
                // deserialize its metadata
                auto metadata = Metadata{};
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
                auto data = requestData(
                    partition, metadata,
                    descriptor);
                // create the event
                auto event = Event{
                    std::make_shared<MofkaEvent>(
                        eventID, partition,
                        std::move(metadata), std::move(data),
                        m_name, m_consumer_ack_event
                )};
                // set the promise
                promises[i].setValue(std::move(event));
            } catch(const Exception& ex) {
                // something bad happened somewhere,
                // pass the exception to the promise.
                promises[i].setException(ex);
            }
            metadata_offset  += batch->m_meta_sizes[i];
            data_desc_offset += batch->m_data_desc_sizes[i];
        }
    };
    m_thread_pool.pushWork(std::move(ult));
}

Data MofkaConsumer::requestData(
        SP<MofkaPartitionInfo> partition,
        Metadata metadata,
        const DataDescriptor& descriptor) {

    // run data selector
    DataDescriptor requested_descriptor = m_data_selector
        ? m_data_selector(metadata, descriptor)
        : DataDescriptor::Null();

    // run data broker
    auto data = m_data_broker
        ? m_data_broker(metadata, requested_descriptor)
        : Data{};

    // check the size of the allocated data
    if(data.size() != requested_descriptor.size()) {
        throw Exception(
                "DataBroker returned a Data object with a "
                "size different from the selected DataDescriptor size");
    }
    if(data.size() == 0) {
        return data;
    }

    // expose the local_data_partition for RDMA
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
    auto& rpc = m_consumer_request_data;
    auto& ph  = partition->m_ph;

    Result<std::vector<Result<void>>> result = rpc.on(ph)(
            Cerealized<DataDescriptor>(requested_descriptor),
            local_bulk_ref);

    if(!result.success())
        throw Exception(result.error());

    if(!result.value()[0].success()) {
        throw Exception(result.value()[0].error());
    }

    return data;
}

void MofkaConsumer::forwardBatchToConsumer(
        const thallium::request& req,
        intptr_t consumer_ctx,
        size_t target_info_index,
        size_t count,
        EventID firstID,
        const BulkRef &metadata_sizes,
        const BulkRef &metadata,
        const BulkRef &data_desc_sizes,
        const BulkRef &data_desc) {
    MofkaConsumer* consumer_impl = reinterpret_cast<MofkaConsumer*>(consumer_ctx);
    if(consumer_impl->m_magic_number != MOFKA_MAGIC_NUMBER) {
        Result<void> result;
        result.error() = "Consumer seems to have be destroyed be client";
        result.success() = false;
        req.respond(result);
    } else {
        // NOTE: we convert the pointer into a shared pointer to prevent
        // the consumer from disappearing while the RPC executes.
        std::shared_ptr<MofkaConsumer> consumer_ptr;
        try {
            consumer_ptr = consumer_impl->shared_from_this();
        } catch(const std::exception& ex) {
            Result<void> result;
            result.error() = "Consumer seems to have be destroyed be client";
            result.success() = false;
            req.respond(result);
            return;
        }
        // in this code path, req.respond() will be called by recvBatch
        if (count == 0) {
            consumer_ptr->partitionCompleted();
            Result<void> result;
            req.respond(result);
        } else {
            consumer_ptr->recvBatch(
                req, target_info_index, count, firstID,
                metadata_sizes, metadata,
                data_desc_sizes, data_desc);
        }
    }
}

}
