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
#include <queue>
#include <deque>
#include <memory>
#include <algorithm>
#include <atomic>

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

    // ack_early config
    bool                m_ack_early = false;
    size_t              m_max_pending_batches = 8;

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

    // ID assignment (atomic, advances before writes when ack_early is enabled)
    std::atomic<size_t>          m_assigned_events{0};

    // Pending write queue for ack_early
    struct PendingWrite {
        std::vector<size_t> metadata_sizes;
        std::vector<char>   metadata_content;
        std::vector<size_t> data_sizes;
        std::vector<char>   data_content;
        size_t              num_events;
        diaspora::EventID   first_id;
    };

    std::queue<PendingWrite>         m_pending_writes;
    thallium::mutex                  m_pending_writes_mtx;
    thallium::condition_variable     m_pending_writes_cv;
    thallium::condition_variable     m_pending_writes_ready_cv;
    bool                             m_writer_stop = false;
    thallium::eventual<void>         m_writer_done;

    // Helpers
    std::string chunkPath(uint32_t chunk_id, const std::string& ext) const;
    void openChunk(uint32_t chunk_id);
    void closeCurrentChunk();
    void rotateChunk();
    bool shouldRotate() const;

    struct WriteBatchResult {
        std::vector<IndexRecord> records;
        std::vector<char>        desc_buf;
        std::vector<size_t>      desc_sizes;
        uint32_t                 chunk_id;
        bool                     success = true;
        std::string              error;
    };

    WriteBatchResult writeBatchToFiles(
        size_t num_events,
        const size_t* metadata_sizes, const char* metadata_content, size_t metadata_content_size,
        const size_t* data_sizes, const char* data_content, size_t data_content_size);

    void readMetadataFromDisk(diaspora::EventID first_id, size_t count,
                              size_t* sizes_out, char* content_out);
    void readDescriptorsFromDisk(diaspora::EventID first_id, size_t count,
                                 size_t* sizes_out, char* content_out);
    void readDataFromDisk(const std::vector<diaspora::DataDescriptor>& descriptors,
                          char* buffer, size_t total_size,
                          std::vector<Result<void>>& results);

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

    bool supportsAckEarly() const override { return m_ack_early; }

    Result<diaspora::EventID> receiveBatchAckEarly(
            const thallium::endpoint& sender,
            const std::string& producer_name,
            size_t num_events,
            const BulkRef& metadata_bulk,
            const BulkRef& data_bulk) override;

    void backgroundWriterLoop();
    void processPendingWrite(PendingWrite& pw);

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
