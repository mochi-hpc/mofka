/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DEFAULT_TOPIC_MANAGER_HPP
#define DEFAULT_TOPIC_MANAGER_HPP

#include <mofka/UUID.hpp>
#include <mofka/DataStore.hpp>
#include <mofka/TopicManager.hpp>

namespace mofka {

/**
 * Default implementation of a mofka TopicManager.
 */
class DefaultTopicManager : public mofka::TopicManager {

    Metadata m_config;
    Metadata m_validator;
    Metadata m_selector;
    Metadata m_serializer;

    thallium::engine m_engine;

    std::vector<char>            m_events_metadata;
    std::vector<size_t>          m_events_metadata_offsets;
    std::vector<size_t>          m_events_metadata_sizes;
    std::vector<char>            m_events_data_desc;
    std::vector<size_t>          m_events_data_desc_offsets;
    std::vector<size_t>          m_events_data_desc_sizes;
    thallium::mutex              m_events_metadata_mtx;
    thallium::mutex              m_events_data_mtx;
    thallium::condition_variable m_events_cv;

    std::unique_ptr<DataStore> m_data_store;

    std::unordered_map<std::string, EventID> m_consumer_cursor;
    thallium::mutex                          m_consumer_cursor_mtx;

    public:

    /**
     * @brief Constructor.
     */
    DefaultTopicManager(
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer,
        thallium::engine engine)
    : m_config(config)
    , m_validator(validator)
    , m_selector(selector)
    , m_serializer(serializer)
    , m_engine(engine) {
        // TODO read the type of DataStore from the configuration
        m_data_store = DataStoreFactory::createDataStore("memory", m_engine, Metadata{"{}"});
    }

    /**
     * @brief Move-constructor.
     */
    DefaultTopicManager(DefaultTopicManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DefaultTopicManager(const DefaultTopicManager&) = delete;

    /**
     * @brief Move-assignment operator.
     */
    DefaultTopicManager& operator=(DefaultTopicManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DefaultTopicManager& operator=(const DefaultTopicManager&) = delete;

    /**
     * @brief Destructor.
     */
    virtual ~DefaultTopicManager() = default;

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
     * @brief Wake up the TopicManager's blocked ConsumerHandles.
     */
    void wakeUp() override;

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
     * @see TopicManager::getData.
     */
    RequestResult<void> getData(
          const std::vector<DataDescriptor>& descriptors,
          const BulkRef& bulk) override;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    mofka::RequestResult<bool> destroy() override;

    /**
     * @brief Static factory function used by the TopicFactory to
     * create a DefaultTopicManager.
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
