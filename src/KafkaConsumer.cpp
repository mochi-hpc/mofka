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
    auto subscription = rd_kafka_topic_partition_list_new(m_partitions.size());
    auto n = m_partitions.size();
    for(size_t i=0; i < n; ++i) {
        rd_kafka_topic_partition_list_add(subscription, m_topic->m_name.c_str(), m_partitions[i]->m_id);
    }
    auto subscribton_ptr = std::shared_ptr<rd_kafka_topic_partition_list_t>{
        subscription, rd_kafka_topic_partition_list_destroy};
    auto err = rd_kafka_subscribe(m_kafka_consumer.get(), subscription);
    if (err) throw Exception{"Failed to subscribe to topic: " + std::string{rd_kafka_err2str(err)}};

    // start the polling loop
    m_should_stop = false;
    auto run = [this](){
        while(!m_should_stop) {
            int timeout = 0; // m_thread_pool.size() > 1 ? 0 : 100;
            rd_kafka_message_t* msg = rd_kafka_consumer_poll(
                m_kafka_consumer.get(), timeout);
            if(!msg) {
                // no message, yield and try again later
                tl::thread::yield();
                continue;
            }
            if(msg && msg->err) {
                // TODO error happened, handle it
                rd_kafka_message_destroy(msg);
                tl::thread::yield();
                continue;
            }
            handleReceivedMessage(msg);
            tl::thread::yield();
        }
        m_poll_ult_stopped.set_value();
    };
    m_thread_pool.pushWork(std::move(run));
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
    rd_kafka_headers_t *headers = nullptr;
    auto err = rd_kafka_message_headers(msg, &headers);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (err != RD_KAFKA_RESP_ERR__NOENT) {
            promise.setException(
                Exception{std::string{"Failed to retrieve message header: "} + rd_kafka_err2str(err)});
            return;
        }
    }

    // Check for NoMoreEvents
    const void* value;
    size_t value_size;
    if (headers &&
        rd_kafka_header_get(headers, 0, "NoMoreEvents", &value, &value_size) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        auto ult = [promise=std::move(promise)]() mutable {
            promise.setValue(Event{std::make_shared<KafkaEvent>()});
        };
        auto completed = ++m_completed_partitions;
        // FIXME If partitions are completed there is no reason to continue running the polling thread
        //if(completed == m_partitions.size()) {
            //m_should_stop = true;
        //}
        m_thread_pool.pushWork(std::move(ult), std::numeric_limits<uint64_t>::max()-1);
        rd_kafka_message_destroy(msg);
        return;
    }

    // create the ULT that handles the payload
    auto ult = [this, msg, promise=std::move(promise), topic=topic()]() mutable {
             try {
                 // deserialize its metadata
                 size_t metadata_size = 0;
                 std::memcpy(&metadata_size, msg->payload, sizeof(metadata_size));
                 size_t data_size = msg->len - sizeof(metadata_size) - metadata_size;
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
                    if(seg.size)
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
             // destroy the message
             rd_kafka_message_destroy(msg);
         };
    m_thread_pool.pushWork(std::move(ult), msg->offset);
}

void KafkaConsumer::unsubscribe() {
    if(!m_should_stop) {
        m_should_stop = true;
        m_poll_ult_stopped.wait();
    }
    rd_kafka_unsubscribe(m_kafka_consumer.get());
}

}
