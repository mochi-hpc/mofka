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
 * Grow-only buffer with a pre-registered RDMA bulk handle (read-only).
 * Re-registers the bulk only when the vector's capacity grows.
 * Non-copyable; move-safe because std::vector move transfers the data pointer.
 */
class BulkCacheEntry {

    std::vector<char>   m_buffer;
    thallium::bulk      m_bulk;
    size_t              m_registered_capacity = 0;
    thallium::engine    m_engine;

public:

    BulkCacheEntry() = default;
    explicit BulkCacheEntry(thallium::engine engine) : m_engine(std::move(engine)) {}
    BulkCacheEntry(BulkCacheEntry&&) = default;
    BulkCacheEntry& operator=(BulkCacheEntry&&) = default;
    BulkCacheEntry(const BulkCacheEntry&) = delete;
    BulkCacheEntry& operator=(const BulkCacheEntry&) = delete;

    char* resize(size_t size) {
        m_buffer.resize(size);
        if(m_buffer.capacity() != m_registered_capacity) {
            if(m_buffer.capacity() > 0) {
                m_bulk = m_engine.expose(
                    {{m_buffer.data(), m_buffer.capacity()}},
                    thallium::bulk_mode::read_only);
                m_registered_capacity = m_buffer.capacity();
            }
        }
        return m_buffer.data();
    }

    char* data() { return m_buffer.data(); }
    const thallium::bulk& bulk() const { return m_bulk; }
};

/**
 * Pool of BulkCacheEntry objects for concurrent getData calls.
 * Checkout takes from the free list or creates a new entry; never blocks.
 * Entries are heap-allocated (unique_ptr) so their addresses — and the
 * data pointers their registered bulks point to — remain stable.
 */
class BulkCachePool {

    std::deque<std::unique_ptr<BulkCacheEntry>> m_free;
    thallium::mutex                              m_mtx;
    thallium::engine                             m_engine;

public:

    BulkCachePool() = default;
    BulkCachePool(thallium::engine engine, size_t initial_size = 0)
    : m_engine(std::move(engine)) {
        if(initial_size > 0) {
            auto entry = std::make_unique<BulkCacheEntry>(m_engine);
            entry->resize(initial_size);
            m_free.push_back(std::move(entry));
        }
    }

    std::unique_ptr<BulkCacheEntry> checkout() {
        std::unique_lock<thallium::mutex> g{m_mtx};
        if(!m_free.empty()) {
            auto e = std::move(m_free.front());
            m_free.pop_front();
            return e;
        }
        return std::make_unique<BulkCacheEntry>(m_engine);
    }

    void checkin(std::unique_ptr<BulkCacheEntry> entry) {
        std::unique_lock<thallium::mutex> g{m_mtx};
        m_free.push_back(std::move(entry));
    }
};

/**
 * RAII guard for a checked-out BulkCachePool entry.
 * Returns the entry to the pool on destruction.
 */
class PooledEntry {

    BulkCachePool&                  m_pool;
    std::unique_ptr<BulkCacheEntry> m_entry;

public:

    explicit PooledEntry(BulkCachePool& pool)
    : m_pool(pool), m_entry(pool.checkout()) {}
    ~PooledEntry() { m_pool.checkin(std::move(m_entry)); }
    BulkCacheEntry& get() { return *m_entry; }
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
    size_t               m_registered_sizes_bytes = 0;
    size_t               m_registered_content_bytes = 0;
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
        // Ensure content has at least 1 byte so the two-segment bulk always
        // has valid memory for both segments.
        size_t effective_content = std::max(content_size, (size_t)1);
        m_sizes.resize(num_events);
        m_content.resize(effective_content);

        size_t exact_sizes_bytes = num_events * sizeof(size_t);
        bool needs_reregister =
            (exact_sizes_bytes != m_registered_sizes_bytes) ||
            (effective_content != m_registered_content_bytes);

        if(needs_reregister && num_events > 0) {
            m_bulk = m_engine.expose(
                {{(char*)m_sizes.data(), exact_sizes_bytes},
                 {m_content.data(), effective_content}},
                m_mode);
            m_registered_sizes_bytes = exact_sizes_bytes;
            m_registered_content_bytes = effective_content;
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

    struct CachedBatch {
        diaspora::EventID        first_id;
        size_t                   num_events;
        uint32_t                 chunk_id;
        std::vector<size_t>      metadata_sizes;
        std::vector<char>        metadata_content;
        std::vector<size_t>      data_sizes;
        std::vector<char>        data_content;
        std::vector<char>        desc_content;
        std::vector<size_t>      desc_sizes;
        std::vector<IndexRecord> index_records;

        size_t memoryUsage() const {
            return metadata_sizes.capacity() * sizeof(size_t)
                 + metadata_content.capacity()
                 + data_sizes.capacity() * sizeof(size_t)
                 + data_content.capacity()
                 + desc_content.capacity()
                 + desc_sizes.capacity() * sizeof(size_t)
                 + index_records.capacity() * sizeof(IndexRecord);
        }
    };

    class WriteCache {
        std::deque<std::shared_ptr<CachedBatch>> m_batches;
        size_t m_max_batches;
        size_t m_max_memory;
        size_t m_current_memory = 0;

    public:
        WriteCache(size_t max_batches = 16, size_t max_memory = 64*1024*1024)
        : m_max_batches(max_batches)
        , m_max_memory(max_memory)
        {}

        void insert(CachedBatch batch) {
            auto ptr = std::make_shared<CachedBatch>(std::move(batch));
            m_current_memory += ptr->memoryUsage();
            m_batches.push_back(std::move(ptr));
            while(m_batches.size() > m_max_batches || m_current_memory > m_max_memory) {
                if(m_batches.empty()) break;
                m_current_memory -= m_batches.front()->memoryUsage();
                m_batches.pop_front();
            }
        }

        void clear() {
            m_batches.clear();
            m_current_memory = 0;
        }

        std::vector<std::shared_ptr<const CachedBatch>> findOverlapping(
                diaspora::EventID first_id, size_t count) const {
            std::vector<std::shared_ptr<const CachedBatch>> result;
            diaspora::EventID last_id = first_id + count;
            for(auto& b : m_batches) {
                diaspora::EventID b_first = b->first_id;
                diaspora::EventID b_last = b->first_id + b->num_events;
                if(b_first < last_id && b_last > first_id) {
                    result.push_back(b);
                }
            }
            return result;
        }

        bool coversRange(diaspora::EventID first_id, size_t count) const {
            if(count == 0) return true;
            auto hits = findOverlapping(first_id, count);
            if(hits.empty()) return false;
            // Check that hits fully cover [first_id, first_id + count)
            diaspora::EventID cursor = first_id;
            diaspora::EventID end = first_id + count;
            for(auto& b : hits) {
                if(b->first_id > cursor) return false;
                diaspora::EventID b_end = b->first_id + b->num_events;
                if(b_end > cursor) cursor = b_end;
                if(cursor >= end) return true;
            }
            return cursor >= end;
        }

        std::pair<const char*, std::shared_ptr<const CachedBatch>>
        findDataByLocation(uint32_t chunk_id, uint64_t offset, uint32_t size) const {
            for(auto& b : m_batches) {
                if(b->chunk_id != chunk_id) continue;
                // Compute the base data offset of this batch from index_records
                if(b->index_records.empty()) continue;
                uint64_t batch_data_base = b->index_records[0].data_offset;
                if(offset < batch_data_base) continue;
                uint64_t rel_offset = offset - batch_data_base;
                if(rel_offset + size > b->data_content.size()) continue;
                return {b->data_content.data() + rel_offset, b};
            }
            return {nullptr, nullptr};
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

    // Buffer pool for getData (pre-registered, concurrent-safe)
    BulkCachePool                m_getdata_pool;

    // Write-through cache
    bool                         m_write_cache_enabled = true;
    WriteCache                   m_write_cache;
    thallium::mutex              m_write_cache_mtx;
    std::atomic<size_t>          m_feed_cache_hits{0};
    std::atomic<size_t>          m_feed_cache_misses{0};
    std::atomic<size_t>          m_getdata_cache_hits{0};
    std::atomic<size_t>          m_getdata_cache_misses{0};

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
    , m_getdata_pool(m_engine, bulk_cache_enabled ? initial_buffer_size : 0)
    {
        if(m_bulk_cache_enabled && m_initial_buffer_size > 0) {
            m_recv_metadata_cache.prepare(0, m_initial_buffer_size);
            m_recv_data_cache.prepare(0, m_initial_buffer_size);
            m_feed_metadata_cache.prepare(0, m_initial_buffer_size);
            m_feed_desc_cache.prepare(0, m_initial_buffer_size);
            // m_getdata_pool is pre-populated via its constructor
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
