/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/KafkaDriver.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"

#include "JsonUtil.hpp"
#include "KafkaDriverImpl.hpp"
#include "KafkaTopicHandle.hpp"
#include "KafkaPartitionInfo.hpp"

#include <fstream>

namespace mofka {

KafkaDriver::KafkaDriver(const std::string& config_file) {
    std::ifstream inputFile(config_file);
    if(!inputFile.is_open()) {
        throw Exception{"Could not open config file"};
    }
    try {
        nlohmann::json config;
        inputFile >> config;
        if(!config.is_object()) {
            throw Exception{"KafkaDriver configuration file doesn't have a correct format"};
        }
        if(!config.contains("bootstrap.servers")
        || !config["bootstrap.servers"].is_string()) {
            throw Exception{"\"bootstrap.servers\" not found or not a string in KafkaDriver configuration"};
        }
        auto& bootstrap_server = config["bootstrap.servers"].get_ref<const std::string&>();
        self = std::make_shared<KafkaDriverImpl>(bootstrap_server);
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

void KafkaDriver::createTopic(
        std::string_view name,
        size_t num_partitions,
        size_t replication_factor,
        Metadata config,
        Validator validator,
        PartitionSelector selector,
        Serializer serializer) {

    if(!self) throw Exception{"Invalid KafkaDriver handle"};

    if(!config.json().is_object())
        throw Exception{"Invalid config passed to KafkaDriver::createTopic: should be an array"};

    auto kconf = rd_kafka_conf_dup(self->m_kafka_config);

    char errstr[512];
    rd_kafka_t *rk = rd_kafka_new(
            RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(kconf);
        throw Exception{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Create the NewTopic object
    auto new_topic = rd_kafka_NewTopic_new(
        name.data(), num_partitions, replication_factor, errstr, sizeof(errstr));
    if (!new_topic) throw Exception{"Failed to create NewTopic object: " + std::string{errstr}};
    auto _new_topic = std::shared_ptr<rd_kafka_NewTopic_s>{new_topic, rd_kafka_NewTopic_destroy};

    // Create an admin options object
    auto options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    if (!options) throw Exception{"Failed to create rd_kafka_AdminOptions_t"};
    auto _options = std::shared_ptr<rd_kafka_AdminOptions_s>{options, rd_kafka_AdminOptions_destroy};

    // Create a queue for the result of the operation
    auto queue = rd_kafka_queue_new(rk);
    auto _queue = std::shared_ptr<rd_kafka_queue_t>{queue, rd_kafka_queue_destroy};

    // Initiate the topic creation
    rd_kafka_NewTopic_t *new_topics[] = {new_topic};
    rd_kafka_CreateTopics(rk, new_topics, 1, options, queue);

    // Wait for the result for up to 10 seconds
    auto event = rd_kafka_queue_poll(queue, 10000);
    if (!event) throw Exception{"Timed out waiting for CreateTopics result"};
    auto _event = std::shared_ptr<rd_kafka_event_t>{event, rd_kafka_event_destroy};

    // Check if the event type is CreateTopics result
    if (rd_kafka_event_type(event) != RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        throw Exception{"Unexpected event type when waiting for CreateTopics"};

    // Extract the result from the event
    auto result = rd_kafka_event_CreateTopics_result(event);
    size_t topic_count;
    auto topics_result = rd_kafka_CreateTopics_result_topics(result, &topic_count);

    // Check the results for errors
    for (size_t i = 0; i < topic_count; i++) {
        const rd_kafka_topic_result_t *topic_result = topics_result[i];
        if (rd_kafka_topic_result_error(topic_result) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw Exception{"Failed to create topic: "
                + std::string{rd_kafka_err2str(rd_kafka_topic_result_error(topic_result))}};
        }
    }
}


TopicHandle KafkaDriver::openTopic(std::string_view name) {
    if(!self) throw Exception{"Invalid KafkaDriver handle"};

    std::vector<std::shared_ptr<KafkaPartitionInfo>> partitions;
    rd_kafka_t* rk;
    const rd_kafka_metadata_t* metadata;
    char errstr[512];

    auto kconf = rd_kafka_conf_dup(self->m_kafka_config);

    // Create Kafka handle
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, kconf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(kconf);
        throw Exception{std::string{"Error creating Kafka handle: "} + errstr};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Get metadata for the topic
    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 1, NULL, &metadata, 10000);
    if (err) throw Exception{std::string{"Error fetching metadata: "} + rd_kafka_err2str(err)};
    auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{metadata, rd_kafka_metadata_destroy};

    // Check if the topic exists
    const rd_kafka_metadata_topic_t *topic_metadata = NULL;
    for (int i = 0; i < metadata->topic_cnt; i++) {
        if (strcmp(metadata->topics[i].topic, name.data()) == 0) {
            topic_metadata = &metadata->topics[i];
            break;
        }
    }

    if (!topic_metadata) throw Exception{std::string{"Topic \""} + name.data() + "\" does not exist"};

    // Get partition information if the topic exists
    partitions.reserve(topic_metadata->partition_cnt);
    for(int i = 0; i < topic_metadata->partition_cnt; ++i) {
        std::vector<int32_t> replicas;
        replicas.reserve(topic_metadata->partitions[i].replica_cnt);
        for(int j = 0; j < topic_metadata->partitions[i].replica_cnt; ++j) {
            replicas.push_back(topic_metadata->partitions[i].replicas[j]);
        }
        partitions.push_back(
            std::make_shared<KafkaPartitionInfo>(
                topic_metadata->partitions[i].id,
                topic_metadata->partitions[i].leader,
                std::move(replicas)));
    }

    return TopicHandle{
        std::make_shared<KafkaTopicHandle>(
            self, name,
            Validator{},
            PartitionSelector{},
            Serializer{},
            std::move(partitions))};
}

bool KafkaDriver::topicExists(std::string_view name) {
    try {
        openTopic(name);
    } catch(const Exception& ex) {
        if(std::string_view{ex.what()}.find("does not exist")) {
            return false;
        }
        throw;
    }
    return true;
}

}
