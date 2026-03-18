/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DEFAULT_PARTITION_MANAGER_HPP
#define DEFAULT_PARTITION_MANAGER_HPP

#include "PartitionManager.hpp"
#include <diaspora/DataDescriptor.hpp>
#include <abt-io.h>
#include <cstdint>
#include <string>

namespace mofka {

/**
 * Default file-based implementation of a mofka PartitionManager.
 * Stores events in append-only chunk files using ABT-IO.
 */
class DefaultPartitionManager : public mofka::PartitionManager {

    public:

    struct IndexRecord {
        uint64_t metadata_offset;
        uint32_t metadata_size;
        uint64_t data_offset;
        uint32_t data_size;
        uint64_t data_desc_offset;
        uint32_t data_desc_size;
    };

    struct FileDataDescriptor {
        uint32_t chunk_id;
        uint64_t offset;
        uint32_t size;

        std::string_view toString() const {
            return std::string_view{reinterpret_cast<const char*>(this), sizeof(*this)};
        }

        static FileDataDescriptor fromDataDescriptor(const diaspora::DataDescriptor& desc) {
            FileDataDescriptor fdd;
            std::memcpy(&fdd, desc.location().data(), sizeof(fdd));
            return fdd;
        }
    };

    private:

    // Config
    std::string         m_path;
    size_t              m_max_chunk_size;
    size_t              m_max_events_per_chunk;
    bool                m_sync;

    // ABT-IO
    abt_io_instance_id  m_abt_io;

    // Engine
    thallium::engine    m_engine;

    // Current chunk write state
    uint32_t            m_current_chunk_id = 0;
    int                 m_fd_meta = -1;
    int                 m_fd_data = -1;
    int                 m_fd_desc = -1;
    int                 m_fd_idx  = -1;
    uint64_t            m_meta_offset = 0;
    uint64_t            m_data_offset = 0;
    uint64_t            m_desc_offset = 0;
    size_t              m_events_in_current_chunk = 0;

    // In-memory index cache
    std::vector<IndexRecord>  m_index;
    std::vector<uint32_t>     m_event_chunk_ids;

    // Event tracking
    size_t                       m_total_events = 0;
    thallium::mutex              m_events_mtx;
    thallium::condition_variable m_events_cv;

    // Consumer cursors
    std::unordered_map<std::string, diaspora::EventID> m_consumer_cursor;
    thallium::mutex                                    m_consumer_cursor_mtx;

    // Write lock
    thallium::mutex              m_write_mtx;

    // Helpers
    std::string chunkPath(uint32_t chunk_id, const std::string& ext) const;
    void openChunk(uint32_t chunk_id);
    void closeCurrentChunk();
    void rotateChunk();
    bool shouldRotate() const;

    public:

    DefaultPartitionManager(
        std::string path,
        size_t max_chunk_size,
        size_t max_events_per_chunk,
        bool sync,
        abt_io_instance_id abt_io,
        thallium::engine engine)
    : m_path(std::move(path))
    , m_max_chunk_size(max_chunk_size)
    , m_max_events_per_chunk(max_events_per_chunk)
    , m_sync(sync)
    , m_abt_io(abt_io)
    , m_engine(std::move(engine))
    {}

    DefaultPartitionManager(DefaultPartitionManager&&) = default;
    DefaultPartitionManager(const DefaultPartitionManager&) = delete;
    DefaultPartitionManager& operator=(DefaultPartitionManager&&) = default;
    DefaultPartitionManager& operator=(const DefaultPartitionManager&) = delete;

    virtual ~DefaultPartitionManager();

    Result<diaspora::EventID> receiveBatch(
            const thallium::endpoint& sender,
            const std::string& producer_name,
            size_t num_events,
            const BulkRef& metadata_bulk,
            const BulkRef& data_bulk) override;

    void wakeUp() override;

    Result<void> feedConsumer(
            ConsumerHandle consumerHandle,
            diaspora::BatchSize batchSize) override;

    Result<void> acknowledge(
          std::string_view consumer_name,
          diaspora::EventID event_id) override;

    Result<std::vector<Result<void>>> getData(
          const std::vector<diaspora::DataDescriptor>& descriptors,
          const BulkRef& bulk) override;

    Result<bool> destroy() override;

    static std::unique_ptr<mofka::PartitionManager> create(
        const thallium::engine& engine,
        const std::string& topic_name,
        const UUID& partition_uuid,
        const diaspora::Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies);
};

}

#endif
