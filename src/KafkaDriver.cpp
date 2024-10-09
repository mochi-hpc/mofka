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

#include <curl/curl.h>
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
        auto rest_url = config.value("rest.url", "");
        self = std::make_shared<KafkaDriverImpl>(bootstrap_server, rest_url);
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
    if(self->m_rest_url.empty())
        throw Exception{"Cannot create topic: no REST proxy URL specified when configuring this KafkaDriver"};

    if(!config.json().is_object())
        throw Exception{"Invalid config passed to KafkaDriver::createTopic: should be an array"};

    CURL *curl;
    CURLcode res;

    // Prepare JSON data for the topic creation
    nlohmann::json topic;
    topic["name"] = std::string{name};
    topic["partitions"] = num_partitions;
    topic["replicationFactor"] = replication_factor;
    topic["config"] = nlohmann::json::object();
    auto& cfg = topic["config"];
    for(auto& item : config.json().items()) {
        cfg[item.key()] = item.value();
    }
    // TODO store the validator, selector, and serializer in a schema registry
    (void)validator;
    (void)selector;
    (void)serializer;

    std::string jsonData = topic.dump(); // Convert to JSON string

    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, (self->m_rest_url + "/topics").c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonData.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER,
                curl_slist_append(NULL, "Content-Type: application/json"));

        // Perform the request
        res = curl_easy_perform(curl);

        if(res != CURLE_OK) {
            curl_easy_cleanup(curl);
            throw Exception{
                "curl_easy_perform() failed: " + std::string{curl_easy_strerror(res)}};
        }

        curl_easy_cleanup(curl);
    }
}

TopicHandle KafkaDriver::openTopic(std::string_view name) {
    if(!self) throw Exception{"Invalid KafkaDriver handle"};
    std::string errstr;

    // Create a producer (or consumer) instance for metadata retrieval
    auto producer = std::unique_ptr<RdKafka::Producer>{
        RdKafka::Producer::create(self->m_kafka_config, errstr)};
    if (!producer)
        throw Exception{"Failed to create Kafka producer to open topic: " + errstr};

    // Fetch metadata
    RdKafka::Metadata* metadata;
    RdKafka::ErrorCode err = producer->metadata(true, nullptr, &metadata, 5000);
    if (err != RdKafka::ERR_NO_ERROR)
        throw Exception{"Failed to retrieve metadata: " + RdKafka::err2str(err)};
    auto metadata_ptr = std::unique_ptr<RdKafka::Metadata>{metadata};

    // Iterate over the topics to find the one we are interested in
    std::vector<std::shared_ptr<KafkaPartitionInfo>> partitions;
    const RdKafka::Metadata::TopicMetadataVector* topics = metadata->topics();
    for (const auto& topic_metadata : *topics) {
        if (topic_metadata->topic() == name) {
            partitions.reserve(topic_metadata->partitions()->size());
            // Iterate over partitions
            for (const auto& partition : *topic_metadata->partitions()) {
                partitions.push_back(
                    std::make_shared<KafkaPartitionInfo>(
                        partition->id(),
                        partition->leader(),
                        *partition->replicas()));
            }
        }
    }

    return TopicHandle{
        std::make_shared<KafkaTopicHandle>(
            self, name,
            Validator{},
            PartitionSelector{},
            Serializer{},
            std::move(partitions))};
}

}
