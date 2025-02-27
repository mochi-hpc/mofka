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
    auto subscription_ptr = std::shared_ptr<rd_kafka_topic_partition_list_t>{
        subscription, rd_kafka_topic_partition_list_destroy};
    auto err = rd_kafka_assign(m_kafka_consumer.get(), subscription);
    if (err) throw Exception{"Failed to subscribe to topic: " + std::string{rd_kafka_err2str(err)}};

    // start the polling loop
    m_should_stop = false;
    auto run = [this](){

        auto queue = rd_kafka_queue_get_consumer(m_kafka_consumer.get());
        auto queue_ptr = std::shared_ptr<rd_kafka_queue_t>(queue, rd_kafka_queue_destroy);
        auto batch_size = this->m_batch_size.value;
        if(batch_size == 0 || batch_size == BatchSize::Adaptive().value)
            batch_size = 1024;

        std::vector<rd_kafka_message_t*> raw_messages(batch_size);

        while(!m_should_stop) {
            int timeout = 100; // m_thread_pool.size() > 1 ? 0 : 100;
            auto msg_received = rd_kafka_consume_batch_queue(
                queue, timeout, raw_messages.data(), raw_messages.size());
            handleReceivedMessages(raw_messages.data(), (size_t)msg_received);
            tl::thread::yield();
        }
        m_poll_ult_stopped.set_value();
    };
    m_thread_pool.pushWork(std::move(run));
}

void KafkaConsumer::handleReceivedMessages(rd_kafka_message_t** msgs, size_t count) {
    if(count == 0) return;
    // get promise/future pairs
    std::vector<Promise<Event>> promises;
    promises.reserve(count);
    {
        std::unique_lock<thallium::mutex> guard{m_futures_mtx};
        for(size_t i = 0; i < count; ++i) {
            Promise<Event> promise;
            if(!m_futures_credit || m_futures.empty()) {
                // the queue of futures is empty or the futures
                // already in the queue have been created by
                // previous calls to handleReceivedMessage() that haven't had
                // a corresponding pull() call from the user.
                Future<Event> future;
                std::tie(future, promise) = Promise<Event>::CreateFutureAndPromise();
                m_futures.emplace_back(promise, std::move(future));
                m_futures_credit = false;
            } else {
                // the queue of futures has futures already
                // created by pull() calls from the user
                promise = std::move(m_futures.front().first);
                m_futures.pop_front();
                m_futures_credit = true;
            }
            promises.emplace_back(std::move(promise));
        }
    }

    ssize_t j = 0;
    for(size_t i = 0; i < count; ++i, ++j) {
        auto msg = msgs[i];
        auto& promise = promises[j];
        auto destroy_msg = std::shared_ptr<rd_kafka_message_t>(msg, rd_kafka_message_destroy);

        if(msg->err) {
            // TODO properly handle the error
            j -= 1;
            continue;
        }

        size_t metadata_size = 0;
        std::memcpy(&metadata_size, msg->payload, sizeof(metadata_size));
        size_t data_size = msg->len - sizeof(metadata_size) - metadata_size;
        if(metadata_size == std::numeric_limits<size_t>::max()) {
            auto completed = ++m_completed_partitions;
            // FIXME If partitions are completed there is no reason to continue running the polling thread
            //if(completed == m_partitions.size()) {
            //m_should_stop = true;
            //}
            if(completed == m_partitions.size()) {
                //m_should_stop = true;
                auto ult = [promise=std::move(promise)]() mutable {
                    promise.setValue(Event{std::make_shared<KafkaEvent>()});
                };
                m_thread_pool.pushWork(std::move(ult), std::numeric_limits<uint64_t>::max()-1);
            }
            continue;
        }

        try {
            auto tpc = topic();
            // deserialize its metadata
            auto metadata = Metadata{};
            BufferWrapperInputArchive metadata_archive{
                std::string_view{
                    static_cast<char*>(msg->payload) + sizeof(size_t),
                        metadata_size}};
            tpc.serializer().deserialize(metadata_archive, metadata);
            // process data associated with the event
            auto descriptor_name = std::string{"kafka:"}
            + tpc.name() + ":" + std::to_string(msg->partition);
            auto descriptor = DataDescriptor::From(descriptor_name, data_size);
            // run data selector
            DataDescriptor requested_descriptor = m_data_selector
                ? m_data_selector(metadata, descriptor)
                : DataDescriptor::Null();
            // run data broker
            auto data = m_data_broker
                ? m_data_broker(metadata, requested_descriptor)
                : Data{};
            if (data.size()) {
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
            }
            // create the event
            auto event = Event{
                std::make_shared<KafkaEvent>(
                        static_cast<EventID>(msg->offset),
                        m_topic->m_partitions[msg->partition],
                        std::move(metadata), std::move(data),
                        shared_from_this())};
            // set the promise
            promise.setValue(std::move(event));
        } catch(const Exception& ex) {
            // something bad happened somewhere,
            // pass the exception to the promise.
            promise.setException(ex);
        }
    }
}

void KafkaConsumer::unsubscribe() {
    if(!m_should_stop) {
        m_should_stop = true;
        m_poll_ult_stopped.wait();
    }
    rd_kafka_unsubscribe(m_kafka_consumer.get());
}

}
