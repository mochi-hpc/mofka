/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_PRODUCER_BATCH_IMPL_H
#define MOFKA_KAFKA_PRODUCER_BATCH_IMPL_H

#include "KafkaPartitionInfo.hpp"
#include "KafkaProducer.hpp"
#include "Promise.hpp"
#include "DataImpl.hpp"
#include "PimplUtil.hpp"
#include "ProducerBatchInterface.hpp"

#include "mofka/BulkRef.hpp"
#include "mofka/Result.hpp"
#include "mofka/EventID.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Data.hpp"
#include "mofka/Future.hpp"
#include "mofka/Producer.hpp"
#include "mofka/BufferWrapperArchive.hpp"

#include <librdkafka/rdkafka.h>
#include <thallium.hpp>
#include <mutex>
#include <queue>
#include <vector>
#include <cstdint>

namespace mofka {

namespace tl = thallium;

class KafkaProducerBatch : public ProducerBatchInterface {

    std::shared_ptr<rd_kafka_t>       m_kafka_prod;
    std::shared_ptr<rd_kafka_topic_t> m_kafka_topic;
    int32_t                           m_partition;

    struct Message {

        std::vector<char> m_payload;
        Promise<EventID>  m_promise;
        std::function<void(rd_kafka_t*, const rd_kafka_message_t*)> m_on_delivered;

        Message(Promise<EventID>&& promise)
        : m_promise{std::move(promise)} {}
        Message(Message&&) = default;
        Message(const Message&) = delete;
        Message& operator=(Message&&) = default;
        Message& operator=(const Message&) = delete;
    };

    std::vector<Message>  m_messages;
    std::atomic<uint64_t> m_msg_sent = 0;
    tl::eventual<void>    m_batch_sent;

    public:

    KafkaProducerBatch(
        std::shared_ptr<rd_kafka_t> kprod,
        std::shared_ptr<rd_kafka_topic_t> ktopic,
        int32_t partition)
    : m_kafka_prod{std::move(kprod)}
    , m_kafka_topic{std::move(ktopic)}
    , m_partition{partition} {}

    void push(
            const Metadata& metadata,
            const Serializer& serializer,
            const Data& data,
            Promise<EventID> promise) override {
        Message msg{std::move(promise)};
        // make space at the beginning of the message to
        // store the size of the metadata
        msg.m_payload.resize(sizeof(size_t));
        // serialize the metadata
        BufferWrapperOutputArchive archive(msg.m_payload);
        serializer.serialize(archive, metadata);
        size_t metadata_size = msg.m_payload.size() - sizeof(size_t);
        std::memcpy(msg.m_payload.data(), &metadata_size, sizeof(metadata_size));
        // serialize the data, one segment at a time
        size_t offset = msg.m_payload.size();
        msg.m_payload.resize(msg.m_payload.size() + data.size());
        for(const auto& seg : data.segments()) {
            std::memcpy(msg.m_payload.data() + offset, seg.ptr, seg.size);
            offset += seg.size;
        }
        // push the message
        m_messages.push_back(std::move(msg));
    }

    void send() override {
        auto n = count();
        if(n == 0) return;
        // Create an array of messages to produce
        std::vector<rd_kafka_message_t> messages(n);
        for(size_t i = 0; i < n; ++i) {

            m_messages[i].m_on_delivered = [this,n,i](rd_kafka_t*, const rd_kafka_message_t* msg) {
                if(!msg->err) {
                    m_messages[i].m_promise.setValue((EventID)msg->offset);
                } else {
                    auto ex = Exception{
                        std::string{"Failed to send event: "} + rd_kafka_err2str(msg->err)};
                    m_messages[i].m_promise.setException(std::move(ex));
                }
                if(++m_msg_sent == n)
                    m_batch_sent.set_value();
            };
            messages[i].payload = m_messages[i].m_payload.data();
            messages[i].len     = m_messages[i].m_payload.size();
            messages[i].key = NULL;
            messages[i].key_len = 0;
            messages[i].partition = m_partition;
            messages[i]._private = static_cast<void*>(&m_messages[i].m_on_delivered);
        }
        // Produce the messages
        auto msg_count = rd_kafka_produce_batch(
            m_kafka_topic.get(), m_partition, 0, messages.data(), n);
        if ((unsigned long)msg_count != n)
            throw Exception{std::string{"Failed to produce all messages: "}
                           + rd_kafka_err2str(rd_kafka_last_error())};
        // Wait for delivery report
        rd_kafka_flush(m_kafka_prod.get(), 0);
        m_batch_sent.wait();
        m_msg_sent = 0;
        m_batch_sent.reset();
        m_messages.clear();
    }

    size_t count() const override {
        return m_messages.size();
    }
};

}

#endif
