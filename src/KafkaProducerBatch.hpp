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

class KafkaBatchProducer;

class KafkaProducerBatch : public ProducerBatchInterface {

    const KafkaBatchProducer*         m_owner;
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
        const KafkaBatchProducer* owner,
        std::shared_ptr<rd_kafka_t> kprod,
        std::shared_ptr<rd_kafka_topic_t> ktopic,
        int32_t partition)
    : m_owner{owner}
    , m_kafka_prod{std::move(kprod)}
    , m_kafka_topic{std::move(ktopic)}
    , m_partition{partition} {}

    void push(
            const Metadata& metadata,
            const Serializer& serializer,
            const Data& data,
            Promise<EventID> promise) override;

    void send() override;

    size_t count() const override {
        return m_messages.size();
    }
};

}

#endif
