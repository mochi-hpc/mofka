/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DEFAULT_TOPIC_MANAGER_HPP
#define DEFAULT_TOPIC_MANAGER_HPP

#include <mofka/UUID.hpp>
#include <mofka/PartitionManager.hpp>
#include "WarabiDataStore.hpp"

namespace mofka {

/**
 * Default implementation of a mofka PartitionManager.
 */
class DefaultPartitionManager : public mofka::PartitionManager {

    Metadata m_config;
    Metadata m_validator;
    Metadata m_selector;
    Metadata m_serializer;

    std::unique_ptr<WarabiDataStore> m_data_store;

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


    std::unordered_map<std::string, EventID> m_consumer_cursor;
    thallium::mutex                          m_consumer_cursor_mtx;

    public:

    /**
     * @brief Constructor.
     */
    DefaultPartitionManager(
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer,
        std::unique_ptr<WarabiDataStore> data_store,
        thallium::engine engine)
    : m_config(config)
    , m_validator(validator)
    , m_selector(selector)
    , m_serializer(serializer)
    , m_data_store(std::move(data_store))
    , m_engine(engine) {}

    /**
     * @brief Move-constructor.
     */
    DefaultPartitionManager(DefaultPartitionManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DefaultPartitionManager(const DefaultPartitionManager&) = delete;

    /**
     * @brief Move-assignment operator.
     */
    DefaultPartitionManager& operator=(DefaultPartitionManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DefaultPartitionManager& operator=(const DefaultPartitionManager&) = delete;

    /**
     * @brief Destructor.
     */
    virtual ~DefaultPartitionManager() = default;

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
    Result<EventID> receiveBatch(
            const thallium::endpoint& sender,
            const std::string& producer_name,
            size_t num_events,
            const BulkRef& metadata_bulk,
            const BulkRef& data_bulk) override;

    /**
     * @brief Wake up the PartitionManager's blocked ConsumerHandles.
     */
    void wakeUp() override;

    /**
     * @see PartitionManager::feedConsumer.
     */
    Result<void> feedConsumer(
            ConsumerHandle consumerHandle,
            BatchSize batchSize) override;

    /**
     * @see PartitionManager::acknowledge.
     */
    Result<void> acknowledge(
          std::string_view consumer_name,
          EventID event_id) override;

    /**
     * @see PartitionManager::getData.
     */
    Result<std::vector<Result<void>>> getData(
          const std::vector<DataDescriptor>& descriptors,
          const BulkRef& bulk) override;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    mofka::Result<bool> destroy() override;

    /**
     * @brief Static factory function used by the TopicFactory to
     * create a DefaultPartitionManager.
     *
     * @param engine Thallium engine
     * @param config Metadata configuration for the manager.
     * @param validator Metadata of the topic's Validator.
     * @param serializer Metadata of the topic's Serializer.
     *
     * @return a unique_ptr to a PartitionManager.
     */
    static std::unique_ptr<mofka::PartitionManager> create(
        const thallium::engine& engine,
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer);

};

}

#endif
