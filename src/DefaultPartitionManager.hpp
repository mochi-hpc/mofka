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
#include <atomic>

namespace mofka {

/**
 * Grow-only buffer cache that reuses an already-registered thallium::bulk handle.
 * Re-registers only when the underlying std::vector reallocates (capacity grows).
 */
class BulkCache {

    std::vector<char>    m_buffer;
    thallium::bulk       m_bulk;
    size_t               m_registered_capacity = 0;
    thallium::engine     m_engine;

public:

    BulkCache() = default;

    BulkCache(thallium::engine engine)
    : m_engine(std::move(engine))
    {}

    // Ensure buffer is at least `size` bytes. Returns pointer to buffer.
    // No bulk registration — used as a simple buffer cache for getData.
    char* resize(size_t size) {
        m_buffer.resize(size);
        return m_buffer.data();
    }

    char* data() { return m_buffer.data(); }
    size_t size() const { return m_buffer.size(); }
};

/**
 * Dual-segment grow-only buffer cache for sizes array + content buffer.
 * Maintains a combined two-segment thallium::bulk handle and re-registers
 * only when either vector's capacity grows.
 */
class DualBulkCache {

    std::vector<size_t>  m_sizes;
    std::vector<char>    m_content;
    thallium::bulk       m_bulk;
    size_t               m_registered_sizes_cap = 0;
    size_t               m_registered_content_cap = 0;
    thallium::engine     m_engine;
    thallium::bulk_mode  m_mode;

public:

    DualBulkCache() = default;

    DualBulkCache(thallium::engine engine, thallium::bulk_mode mode)
    : m_engine(std::move(engine))
    , m_mode(mode)
    {}

    /**
     * Prepare sizes array for num_events and content buffer for content_size bytes.
     * Re-registers the combined bulk only if either vector grew beyond registered capacity.
     */
    void prepare(size_t num_events, size_t content_size) {
        m_sizes.resize(num_events);
        // Ensure content has at least 1 byte so the two-segment bulk always
        // has valid memory for both segments.
        m_content.resize(std::max(content_size, (size_t)1));

        bool needs_reregister =
            (m_sizes.capacity() * sizeof(size_t) > m_registered_sizes_cap) ||
            (m_content.capacity() > m_registered_content_cap);

        if(needs_reregister && num_events > 0) {
            auto sizes_bytes = m_sizes.capacity() * sizeof(size_t);
            auto content_bytes = m_content.capacity();
            m_bulk = m_engine.expose(
                {{(char*)m_sizes.data(), sizes_bytes},
                 {m_content.data(), content_bytes}},
                m_mode);
            m_registered_sizes_cap = sizes_bytes;
            m_registered_content_cap = content_bytes;
        }
    }

    size_t* sizes_data() { return m_sizes.data(); }
    char* content_data() { return m_content.data(); }
    const thallium::bulk& bulk() const { return m_bulk; }

    size_t sizes_bytes() const { return m_sizes.size() * sizeof(size_t); }
    size_t content_bytes() const { return m_content.size(); }
};

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
    thallium::condition_variable     m_pending_writes_cv;       // backpressure
    thallium::condition_variable     m_pending_writes_ready_cv; // signal writer
    bool                             m_writer_stop = false;
    thallium::eventual<void>         m_writer_done;

    // Bulk cache config
    bool                         m_bulk_cache_enabled = true;
    size_t                       m_initial_buffer_size = 0;

    // Buffer caches for receiveBatch (write-only: receiving from producer via RDMA pull)
    DualBulkCache                m_recv_metadata_cache;
    DualBulkCache                m_recv_data_cache;

    // Buffer caches for feedConsumer (read-only: sending to consumer)
    DualBulkCache                m_feed_metadata_cache;
    DualBulkCache                m_feed_desc_cache;

    // Buffer cache for getData (buffer only, no bulk caching)
    BulkCache                    m_getdata_cache;
    thallium::mutex              m_getdata_mtx;

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
        thallium::engine engine,
        bool bulk_cache_enabled = true,
        size_t initial_buffer_size = 0)
    : m_path(std::move(path))
    , m_max_chunk_size(max_chunk_size)
    , m_max_events_per_chunk(max_events_per_chunk)
    , m_sync(sync)
    , m_abt_io(abt_io)
    , m_engine(std::move(engine))
    , m_bulk_cache_enabled(bulk_cache_enabled)
    , m_initial_buffer_size(initial_buffer_size)
    , m_recv_metadata_cache(m_engine, thallium::bulk_mode::write_only)
    , m_recv_data_cache(m_engine, thallium::bulk_mode::write_only)
    , m_feed_metadata_cache(m_engine, thallium::bulk_mode::read_only)
    , m_feed_desc_cache(m_engine, thallium::bulk_mode::read_only)
    , m_getdata_cache(m_engine)
    {
        if(m_bulk_cache_enabled && m_initial_buffer_size > 0) {
            m_recv_metadata_cache.prepare(0, m_initial_buffer_size);
            m_recv_data_cache.prepare(0, m_initial_buffer_size);
            m_feed_metadata_cache.prepare(0, m_initial_buffer_size);
            m_feed_desc_cache.prepare(0, m_initial_buffer_size);
            m_getdata_cache.resize(m_initial_buffer_size);
        }
    }

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
