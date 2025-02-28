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
#include "KafkaBatchProducer.hpp"
#include "KafkaDriverImpl.hpp"
#include "KafkaConsumer.hpp"

namespace mofka {

static inline void checkOptions(const Metadata& options) {
    auto& options_json = options.json();
    if(!options_json.is_object()) {
        throw Exception{"Producer options should be a JSON object"};
    }
    for(auto& p : options_json.items()) {
        if(!p.value().is_string()) {
            throw Exception{"Producer options should only have string values"};
        }
    }
}

static inline void addOptions(const Metadata& options, rd_kafka_conf_t* config) {
    auto& options_json = options.json();
    char errstr[512];
    for(auto& p : options_json.items()) {
        auto ret = rd_kafka_conf_set(config,
                p.key().c_str(), p.value().get_ref<const std::string&>().c_str(),
                errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
            throw Exception{"Could not set options \"" + p.key() + "\": " + errstr};
    }
}


Producer KafkaTopicHandle::makeProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering,
        Metadata options) const {

    checkOptions(options);

    char errstr[512];
    // Create callback for message delivery
    auto dr_msg_cb = [](rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) -> void {
        (void)opaque;
        if(!rkmessage->_private) return;
        std::function<void(rd_kafka_t*, const rd_kafka_message_t*)>* fn =
            static_cast<decltype(fn)>(rkmessage->_private);
        (*fn)(rk, rkmessage);
    };

    // Create configuration for producer
    auto kconf = rd_kafka_conf_dup(m_driver->m_kafka_config);
    rd_kafka_conf_set_dr_msg_cb(kconf, dr_msg_cb);

    addOptions(options, kconf);

    // Create producer instance
    auto kprod = rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr));
    if (!kprod) throw Exception{"Failed to create Kafka producer: " + std::string{errstr}};
    auto kprod_ptr = std::shared_ptr<rd_kafka_t>{kprod, rd_kafka_destroy};

    rd_kafka_poll(kprod, 0);

    // Create topic object
    auto ktopic = rd_kafka_topic_new(kprod, m_name.data(), NULL);
    if (!ktopic) throw Exception{std::string{"Failed to create Kafka topic object: "}
                                + rd_kafka_err2str(rd_kafka_last_error())};
    auto ktopic_ptr = std::shared_ptr<rd_kafka_topic_t>{ktopic, rd_kafka_topic_destroy};

    // Create the KafkaProducer instance
#if 0
    if(batch_size == BatchSize{0} || batch_size == BatchSize::Adaptive()) {
        return Producer{std::make_shared<KafkaProducer>(
            name, batch_size, std::move(thread_pool), ordering,
            const_cast<KafkaTopicHandle*>(this)->shared_from_this(),
            std::move(kprod_ptr), std::move(ktopic_ptr))};
    } else {
        return Producer{std::make_shared<KafkaBatchProducer>(
            name, batch_size, std::move(thread_pool), ordering,
            const_cast<KafkaTopicHandle*>(this)->shared_from_this(),
            std::move(kprod_ptr), std::move(ktopic_ptr))};
    }
#endif
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
        const std::vector<size_t>& targets,
        Metadata options) const {

    checkOptions(options);

    if(targets.size() != 0) {
        std::cerr << "WARNING: targets argument in KafkaTopic::consumer will be ignored" << std::endl;
    }

    char errstr[512];

    std::vector<SP<KafkaPartitionInfo>> partitions;
    partitions = m_partitions;
#if 0
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
#endif

    // Create configuration for consumer
    auto kconf = rd_kafka_conf_dup(m_driver->m_kafka_config);
    addOptions(options, kconf);
    auto ret = rd_kafka_conf_set(kconf, "group.id", name.data(), errstr, sizeof(errstr));
    if (ret != RD_KAFKA_CONF_OK)
        throw Exception{"Could not set Kafka group.id configuration: " + std::string(errstr)};
    ret = rd_kafka_conf_set(kconf, "enable.auto.commit", "false", errstr, sizeof(errstr));
    if (ret != RD_KAFKA_CONF_OK)
        throw Exception{"Could not set Kafka enable.auto.commit configuration: " + std::string(errstr)};
    ret = rd_kafka_conf_set(kconf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));
    if (ret != RD_KAFKA_CONF_OK)
          throw Exception{"Could not set Kafka auto.offset.reset configuration: " + std::string(errstr)};

    auto rebalance_cb = [](rd_kafka_t* kcons,
                           rd_kafka_resp_err_t err,
                           rd_kafka_topic_partition_list_t* list,
                           void *) -> void {
        rd_kafka_resp_err_t error;
        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            error = rd_kafka_assign(kcons, list);
        } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
            error = rd_kafka_assign(kcons, nullptr);
        } else {
            error = rd_kafka_assign(kcons, nullptr);
        }
        if(error != 0) {
            std::cerr << "rd_kafka_assign failed with error: "
                << rd_kafka_err2str(error) << std::endl;
        }
    };

    rd_kafka_conf_set_rebalance_cb(kconf, rebalance_cb);

    // Create Kafka consumer instance
    auto kcons = rd_kafka_new(RD_KAFKA_CONSUMER, kconf, errstr, sizeof(errstr));
    if (!kcons) throw Exception{"Failed to create Kafka consumer: " + std::string{errstr}};
    auto kcons_ptr = std::shared_ptr<rd_kafka_t>{kcons, rd_kafka_destroy};

    auto consumer = std::make_shared<KafkaConsumer>(
            name, batch_size, std::move(thread_pool),
            data_broker, data_selector,
            const_cast<KafkaTopicHandle*>(this)->shared_from_this(),
            std::move(partitions), kcons_ptr);
    consumer->subscribe();
    return Consumer{std::move(consumer)};
}

void KafkaTopicHandle::markAsComplete() const {

    char errstr[512];
    // Create configuration for temporary producer
    auto kconf = rd_kafka_conf_dup(m_driver->m_kafka_config);

    // Create producer instance
    auto kprod = rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr));
    if (!kprod) throw Exception{"Failed to create Kafka producer: " + std::string{errstr}};
    auto kprod_ptr = std::shared_ptr<rd_kafka_t>{kprod, rd_kafka_destroy};

    // Produce one event per partition
    for (size_t i = 0; i < m_partitions.size(); i++) {
        size_t no_more_events = std::numeric_limits<size_t>::max();
        auto err = rd_kafka_producev(kprod,
                    RD_KAFKA_V_TOPIC(m_name.c_str()),
                    RD_KAFKA_V_PARTITION(m_partitions[i]->m_id),
                    RD_KAFKA_V_VALUE((void*)&no_more_events, sizeof(no_more_events)),
                    RD_KAFKA_V_END);
    }

    // Wait for messages to be delivered
    rd_kafka_flush(kprod, 10000);
}

}
