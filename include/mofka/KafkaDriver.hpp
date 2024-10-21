/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_DRIVER_HPP
#define MOFKA_KAFKA_DRIVER_HPP

#include <mofka/Exception.hpp>
#include <mofka/Serializer.hpp>
#include <mofka/Validator.hpp>
#include <mofka/PartitionSelector.hpp>
#include <mofka/Metadata.hpp>

#include <thallium.hpp>
#include <memory>
#include <unordered_set>

namespace mofka {

class KafkaDriverImpl;

/**
 * @brief A KafkaDriver is the main class used to interact with a Kafka service.
 */
class KafkaDriver {

    public:

    KafkaDriver() = default;

    /**
     * @brief Constructor.
     */
    KafkaDriver(const std::string& config_file);

    /**
     * @brief Copy-constructor.
     */
    KafkaDriver(const KafkaDriver&) = default;

    /**
     * @brief Move-constructor.
     */
    KafkaDriver(KafkaDriver&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    KafkaDriver& operator=(const KafkaDriver&) = default;

    /**
     * @brief Move-assignment operator.
     */
    KafkaDriver& operator=(KafkaDriver&&) = default;

    /**
     * @brief Destructor.
     */
    ~KafkaDriver() = default;

    /**
     * @brief Checks if the KafkaDriver instance is valid.
     */
    operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param num_partitions Number of partitions.
     * @param replication_factor Replication factor.
     * @param config Topic configuration (should be an array of key/value pairs)
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    void createTopic(std::string_view name,
                     size_t num_partitions = 1,
                     size_t replication_factor = 1,
                     Metadata config = Metadata{},
                     Validator validator = Validator{},
                     PartitionSelector selector = PartitionSelector{},
                     Serializer serializer = Serializer{});

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    TopicHandle openTopic(std::string_view name);

    /**
     * @brief Checks if a topic exists.
     */
    bool topicExists(std::string_view name);

    private:

    KafkaDriver(const std::shared_ptr<KafkaDriverImpl>& impl)
    : self{impl} {}

    std::shared_ptr<KafkaDriverImpl> self;
};

}

#endif
