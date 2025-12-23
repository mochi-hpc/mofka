/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DEFAULT_TOPIC_MANAGER_HPP
#define DEFAULT_TOPIC_MANAGER_HPP

#include <mofka/UUID.hpp>
#include "PartitionManager.hpp"
#include "WarabiDataStore.hpp"
#include "YokanEventStore.hpp"

namespace mofka {

/**
 * Default implementation of a mofka PartitionManager.
 */
class DefaultPartitionManager : public mofka::PartitionManager {

    diaspora::Metadata m_config;

    std::unique_ptr<WarabiDataStore> m_data_store;
    std::unique_ptr<YokanEventStore> m_event_store;

    thallium::engine m_engine;

    std::unordered_map<std::string, diaspora::EventID> m_consumer_cursor;
    thallium::mutex                                    m_consumer_cursor_mtx;

    public:

    /**
     * @brief Constructor.
     */
    DefaultPartitionManager(
        const diaspora::Metadata& config,
        std::unique_ptr<WarabiDataStore> data_store,
        std::unique_ptr<YokanEventStore> event_store,
        thallium::engine engine)
    : m_config(config)
    , m_data_store(std::move(data_store))
    , m_event_store(std::move(event_store))
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
     * @brief Receives a batch.
     */
    Result<diaspora::EventID> receiveBatch(
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
            diaspora::BatchSize batchSize) override;

    /**
     * @see PartitionManager::acknowledge.
     */
    Result<void> acknowledge(
          std::string_view consumer_name,
          diaspora::EventID event_id) override;

    /**
     * @see PartitionManager::getData.
     */
    Result<std::vector<Result<void>>> getData(
          const std::vector<diaspora::DataDescriptor>& descriptors,
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
     * @param engine Thallium engine.
     * @param topic_name Topic name.
     * @param partition_uuid Partition UUID..
     * @param config Metadata configuration for the manager.
     * @param dependencies Dependencies provided by Bedrock.
     *
     * @return a unique_ptr to a PartitionManager.
     */
    static std::unique_ptr<mofka::PartitionManager> create(
        const thallium::engine& engine,
        const std::string& topic_name,
        const UUID& partition_uuid,
        const diaspora::Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies);

};

}

#endif
