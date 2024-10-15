/*
 * (C) 2024 The University of Chicago
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
#include "KafkaEvent.hpp"
#include "Promise.hpp"
#include "KafkaConsumer.hpp"
#include "ConsumerBatchImpl.hpp"
#include <limits>

using namespace std::string_literals;

namespace mofka {

TopicHandle KafkaConsumer::topic() const {
    return TopicHandle{m_topic};
}

Future<Event> KafkaConsumer::pull() {
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
            promise.setValue(Event{std::make_shared<KafkaEvent>()});
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

void KafkaConsumer::subscribe() {
    // for each partition, call rd_kafka_consume_start
    auto n = m_partitions.size();
    for(size_t i=0; i < n; ++i) {
        int ret = rd_kafka_consume_start(
            m_kafka_topic.get(), m_partitions[i]->m_id, RD_KAFKA_OFFSET_STORED);
        if (ret != 0) {
            for(size_t j = 0; j < i; ++j) {
                rd_kafka_consume_stop(m_kafka_topic.get(), m_partitions[i]->m_id);
            }
            throw Exception{"Could not subscribe to partition " + std::to_string(m_partitions[i]->m_id)
                            + ": rd_kafka_consume_start returned " + std::to_string(ret)};
       }
    }
    // start the polling loop
    m_should_stop = false;
    auto run = [this](){
        while(!m_should_stop) {
            int timeout = m_thread_pool.size() > 1 ? 0 : 100;
            rd_kafka_message_t* msg = rd_kafka_consumer_poll(
                m_kafka_consumer.get(), timeout);
            if(!msg) {
                // no message, yield and try again later
                tl::thread::yield();
                continue;
            }
            if(msg && msg->err) {
                // TODO error happened, handle it
                tl::thread::yield();
                continue;
            }
            handleReceivedMessage(msg);
            tl::thread::yield();
        }
        m_poll_ult_stopped.set_value();
    };
    m_thread_pool.pushWork(std::move(run), std::numeric_limits<uint64_t>::max());
}

void KafkaConsumer::handleReceivedMessage(rd_kafka_message_t* msg) {
    // get a promise/future pair
    Promise<Event> promise;
    {
        std::unique_lock<thallium::mutex> guard{m_futures_mtx};
        if(!m_futures_credit || m_futures.empty()) {
            // the queue of futures is empty or the futures
            // already in the queue have been created by
            // previous calls to handleReceivedMessage() that haven't had
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

    // Retrieve the headers from the message
    rd_kafka_headers_t *headers;
    rd_kafka_resp_err_t err;
    err = rd_kafka_message_headers(msg, &headers);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (err != RD_KAFKA_RESP_ERR__NOENT) {
            promise.setException(
                Exception{std::string{"Failed to retrieve message header: "} + rd_kafka_err2str(err)});
        }
        return;
    }

    // Check for NoMoreEvents
    const void* value;
    size_t value_size;
    if (rd_kafka_header_get(headers, 0, "NoMoreEvents", &value, &value_size) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        auto ult = [promise=std::move(promise)]() mutable {
            promise.setValue(Event{});
        };
        m_thread_pool.pushWork(std::move(ult), NoMoreEvents);
        return;
    }

    // create the ULT that handles the payload
    auto ult = [this, msg, promise=std::move(promise), topic=topic()]() mutable {
             try {
                 // deserialize its metadata
                 size_t data_size = 0;
                 std::memcpy(&data_size, msg->payload, sizeof(data_size));
                 size_t metadata_size = msg->len - sizeof(data_size) - data_size;
                 auto metadata = Metadata{};
                 BufferWrapperInputArchive metadata_archive{
                     std::string_view{
                         static_cast<char*>(msg->payload) + sizeof(size_t),
                         metadata_size}};
                 topic.serializer().deserialize(metadata_archive, metadata);
                 // process data associated with the event
                 auto descriptor_name = std::string{"kafka:"}
                                      + topic.name() + ":" + std::to_string(msg->partition);
                 auto descriptor = DataDescriptor::From(descriptor_name, data_size);
                 // run data selector
                 DataDescriptor requested_descriptor = m_data_selector
                     ? m_data_selector(metadata, descriptor)
                     : DataDescriptor::Null();
                 // run data broker
                 auto data = m_data_broker
                     ? m_data_broker(metadata, requested_descriptor)
                     : Data{};
                 // flatten descriptor
                 auto flattened = requested_descriptor.flatten();
                 // Copy the data from payload into the data target
                 char* payload_base_ptr = static_cast<char*>(msg->payload) + sizeof(size_t) + metadata_size;
                 size_t data_offset = 0;
                 for(auto& seg : flattened) {
                    auto payload_ptr = payload_base_ptr + seg.offset;
                    if(seg.offset + seg.size > data_size)
                        throw Exception{"Invalid segment in DataDescriptor would read beyond size of data"};
                    data.write(payload_ptr, seg.size, data_offset);
                    data_offset += seg.size;
                 }
                 // create the event
                 auto event = Event{
                     std::make_shared<KafkaEvent>(
                             static_cast<EventID>(msg->offset),
                             m_partitions[msg->partition],
                             std::move(metadata), std::move(data),
                             shared_from_this())};
                 // set the promise
                 promise.setValue(std::move(event));
             } catch(const Exception& ex) {
                 // something bad happened somewhere,
                 // pass the exception to the promise.
                 promise.setException(ex);
             }
         };
    m_thread_pool.pushWork(std::move(ult), msg->offset);
}

void KafkaConsumer::unsubscribe() {
    auto n = m_partitions.size();
    for(size_t i=0; i < n; ++i)
        rd_kafka_consume_stop(m_kafka_topic.get(), m_partitions[i]->m_id);
    if(!m_should_stop) {
        m_should_stop = true;
        m_poll_ult_stopped.wait();
    }
}

#if 0
void KafkaConsumer::recvBatch(size_t partition_index,
                             size_t count,
                             EventID startID,
                             const BulkRef &metadata_sizes,
                             const BulkRef &metadata,
                             const BulkRef &data_desc_sizes,
                             const BulkRef &data_desc) {

    if(count == 0) { // no more events from this partition
        m_completed_partitions += 1;
        // check if there will be no more events any more, if so, set
        // the promise of all the pending Futures to NoMoreEvents.
        std::unique_lock<thallium::mutex> guard{m_futures_mtx};
        if(m_completed_partitions != m_partitions.size()) {
            return;
        }
        if(!m_futures_credit) return;
        while(!m_futures.empty()) {
            auto promise = std::move(m_futures.front().first);
            m_futures.pop_front();
            m_futures_credit = true;
            promise.setValue(Event{std::make_shared<KafkaEvent>()});
        }
        return;
    }

    auto partition = m_partitions[partition_index];

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
        // create the ULT
        auto ult = [this, &batch, i, eventID, promise,
                    partition, metadata_offset, data_desc_offset,
                    &serializer, &ults_completed]() mutable {
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
                    std::make_shared<KafkaEvent>(
                        eventID, std::move(partition),
                        std::move(metadata), std::move(data),
                        m_name, m_consumer_ack_event
                )};
                // set the promise
                promise.setValue(std::move(event));
            } catch(const Exception& ex) {
                // something bad happened somewhere,
                // pass the exception to the promise.
                promise.setException(ex);
            }
            ults_completed.set(nullptr);
        };
        m_thread_pool.pushWork(std::move(ult), eventID);
        metadata_offset  += batch->m_meta_sizes[i];
        data_desc_offset += batch->m_data_desc_sizes[i];
    }
    ults_completed.wait();
}


void KafkaConsumer::forwardBatchToConsumer(
        const thallium::request& req,
        intptr_t consumer_ctx,
        size_t target_info_index,
        size_t count,
        EventID firstID,
        const BulkRef &metadata_sizes,
        const BulkRef &metadata,
        const BulkRef &data_desc_sizes,
        const BulkRef &data_desc) {
    Result<void> result;
    KafkaConsumer* consumer_impl = reinterpret_cast<KafkaConsumer*>(consumer_ctx);
    if(consumer_impl->m_magic_number != MOFKA_MAGIC_NUMBER) {
        result.error() = "Consumer seems to have be destroyed be client";
        result.success() = false;
    } else {
        std::shared_ptr<KafkaConsumer> consumer_ptr;
        try {
            consumer_ptr = consumer_impl->shared_from_this();
        } catch(const std::exception& ex) {
            result.error() = "Consumer seems to have be destroyed be client";
            result.success() = false;
            req.respond(result);
            return;
        }
        // NOTE: we convert the pointer into a shared pointer to prevent
        // the consumer from disappearing while the RPC executes.
        consumer_impl->recvBatch(
                target_info_index, count, firstID,
                metadata_sizes, metadata,
                data_desc_sizes, data_desc);
    }
    req.respond(result);
}
#endif

}
