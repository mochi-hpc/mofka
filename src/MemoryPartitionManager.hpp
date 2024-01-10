/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MEMORY_PARTITION_MANAGER_HPP
#define MEMORY_PARTITION_MANAGER_HPP

#include <mofka/PartitionManager.hpp>
#include <mofka/DataDescriptor.hpp>

namespace mofka {

/**
 * Memory implementation of a mofka PartitionManager.
 */
class MemoryPartitionManager : public mofka::PartitionManager {

    struct OffsetSize {

        size_t offset;
        size_t size;

        std::string_view toString() const {
            return std::string_view{reinterpret_cast<const char*>(this), sizeof(*this)};
        }

        void fromDataDescriptor(const DataDescriptor& desc) {
            std::memcpy(&offset, desc.location().data(), sizeof(offset));
            std::memcpy(&size, desc.location().data() + sizeof(offset), sizeof(size));
        }
    };

    Metadata m_config;

    thallium::engine m_engine;

    std::vector<char>            m_events_metadata;
    std::vector<size_t>          m_events_metadata_offsets;
    std::vector<size_t>          m_events_metadata_sizes;
    std::vector<char>            m_events_data;
    std::vector<size_t>          m_events_data_offsets;
    std::vector<size_t>          m_events_data_sizes;
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
    MemoryPartitionManager(
        const Metadata& config,
        thallium::engine engine)
    : m_config(config)
    , m_engine(engine) {}

    /**
     * @brief Move-constructor.
     */
    MemoryPartitionManager(MemoryPartitionManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    MemoryPartitionManager(const MemoryPartitionManager&) = delete;

    /**
     * @brief Move-assignment operator.
     */
    MemoryPartitionManager& operator=(MemoryPartitionManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    MemoryPartitionManager& operator=(const MemoryPartitionManager&) = delete;

    /**
     * @brief Destructor.
     */
    virtual ~MemoryPartitionManager() = default;

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
     * create a MemoryPartitionManager.
     *
     * @param engine Thallium engine
     * @param topic_name Topic name
     * @param partition_uuid Partition UUID
     * @param config Metadata configuration for the manager.
     * @param dependencies Dependencies provided by Bedrock.
     *
     * @return a unique_ptr to a PartitionManager.
     */
    static std::unique_ptr<mofka::PartitionManager> create(
        const thallium::engine& engine,
        const std::string& topic_name,
        const UUID& partition_uuid,
        const Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies);

};

}

#endif
