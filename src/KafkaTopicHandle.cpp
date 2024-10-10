/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TopicHandle.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"

#include "PimplUtil.hpp"
#include "KafkaTopicHandle.hpp"
#include "KafkaProducer.hpp"
#include "KafkaDriverImpl.hpp"
//#include "KafkaConsumer.hpp"

namespace mofka {

Producer KafkaTopicHandle::makeProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering) const {

    char errstr[512];
    // Create callback for message delivery
    auto dr_msg_cb = [](rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) -> void {
        if(!opaque) return;
        std::function<void(rd_kafka_t*, const rd_kafka_message_t*)>* fn =
            static_cast<decltype(fn)>(opaque);
        (*fn)(rk, rkmessage);
    };

    // Create configuration for producer
    auto kconf = rd_kafka_conf_dup(m_driver->m_kafka_config);
    rd_kafka_conf_set_dr_msg_cb(kconf, dr_msg_cb);

    // Create producer instance
    auto kprod = rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr));
    if (!kprod) throw Exception{"Failed to create Kafka producer: " + std::string{errstr}};
    auto kprod_ptr = std::shared_ptr<rd_kafka_t>{kprod, rd_kafka_destroy};

    // Create topic object
    auto ktopic = rd_kafka_topic_new(kprod, name.data(), NULL);
    if (!ktopic) throw Exception{std::string{"Failed to create Kafka topic object: "}
                                + rd_kafka_err2str(rd_kafka_last_error())};
    auto ktopic_ptr = std::shared_ptr<rd_kafka_topic_t>{ktopic, rd_kafka_topic_destroy};

    // Create the KafkaProducer instance
    return Producer{std::make_shared<KafkaProducer>(
        name, batch_size, std::move(thread_pool), ordering,
        const_cast<KafkaTopicHandle*>(this)->shared_from_this(),
        std::move(kprod_ptr), std::move(ktopic_ptr))};
}

Consumer KafkaTopicHandle::makeConsumer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        DataBroker data_broker,
        DataSelector data_selector,
        const std::vector<size_t>& targets) const {
#if 0
    std::vector<SP<KafkaPartitionInfo>> partitions;
    if(targets.empty()) {
        partitions = m_partitions;
    } else {
        partitions.reserve(targets.size());
        for(auto& partition_index : targets) {
            if(partition_index >= m_partitions.size())
                throw Exception{"Invalid partition index passed to TopicHandle::consumer()"};
            partitions.push_back(m_partitions[partition_index]);
        }
    }
    auto consumer = std::make_shared<KafkaConsumer>(
            m_engine, name, batch_size, std::move(thread_pool),
            data_broker, data_selector,
            const_cast<KafkaTopicHandle*>(this)->shared_from_this(),
            std::move(partitions));
    consumer->subscribe();
    return Consumer{std::move(consumer)};
#endif
}

void KafkaTopicHandle::markAsComplete() const {
#if 0
    auto rpc = m_topic_mark_as_complete;
    std::vector<tl::async_response> responses;
    std::vector<Result<void>> results;
    responses.reserve(m_partitions.size());
    results.reserve(m_partitions.size());
    try {
        for(auto& partition : m_partitions) {
            auto& ph = partition->m_ph;
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
#endif
}

}
