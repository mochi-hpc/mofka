/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DUMMY_TOPIC_MANAGER_HPP
#define DUMMY_TOPIC_MANAGER_HPP

#include <mofka/TopicManager.hpp>

namespace mofka {

/**
 * Dummy implementation of a mofka TopicManager.
 */
class DummyTopicManager : public mofka::TopicManager {

    Metadata m_config;
    Metadata m_validator;
    Metadata m_selector;
    Metadata m_serializer;

    thallium::engine        m_engine;

    std::vector<char>       m_events_metadata;
    std::vector<size_t>     m_events_metadata_offsets;
    std::vector<size_t>     m_events_metadata_sizes;
    std::vector<char>       m_events_data;
    std::vector<size_t>     m_events_data_offsets;
    std::vector<size_t>     m_events_data_sizes;
    thallium::mutex         m_events_mtx;

    /* mapping from a consumer name to the last acknowledged eventID */
    std::unordered_map<std::string, EventID> m_last_ack;
    thallium::mutex                          m_last_ack_mtx;

    public:

    /**
     * @brief Constructor.
     */
    DummyTopicManager(
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer,
        thallium::engine engine)
    : m_config(config)
    , m_validator(validator)
    , m_selector(selector)
    , m_serializer(serializer)
    , m_engine(engine) {}

    /**
     * @brief Move-constructor.
     */
    DummyTopicManager(DummyTopicManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DummyTopicManager(const DummyTopicManager&) = delete;

    /**
     * @brief Move-assignment operator.
     */
    DummyTopicManager& operator=(DummyTopicManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DummyTopicManager& operator=(const DummyTopicManager&) = delete;

    /**
     * @brief Destructor.
     */
    virtual ~DummyTopicManager() = default;

    /**
     * @brief Get the Metadata of the Validator associated with this topic.
     */
    virtual Metadata getValidatorMetadata() const override;

    /**
     * @brief Get the Metadata of the TargetSelector associated with this topic.
     */
    virtual Metadata getTargetSelectorMetadata() const override;

    /**
     * @brief Get the Metadata of the Serializer associated with this topic.
     */
    virtual Metadata getSerializerMetadata() const override;

    /**
     * @brief Receives a batch.
     */
    RequestResult<EventID> receiveBatch(
            const thallium::endpoint& sender,
            const std::string& producer_name,
            size_t num_events,
            const BulkRef& metadata_bulk,
            const BulkRef& data_bulk) override;

    /**
     * @see TopicManager::feedConsumer.
     */
    RequestResult<void> feedConsumer(
            ConsumerHandle consumerHandle,
            BatchSize batchSize) override;

    /**
     * @see TopicManager::acknowledge.
     */
    RequestResult<void> acknowledge(
          std::string_view consumer_name,
          EventID event_id) override;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    mofka::RequestResult<bool> destroy() override;

    /**
     * @brief Static factory function used by the TopicFactory to
     * create a DummyTopicManager.
     *
     * @param engine Thallium engine
     * @param config Metadata configuration for the manager.
     * @param validator Metadata of the topic's Validator.
     * @param serializer Metadata of the topic's Serializer.
     *
     * @return a unique_ptr to a TopicManager.
     */
    static std::unique_ptr<mofka::TopicManager> create(
        const thallium::engine& engine,
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer);

};

}

#endif
