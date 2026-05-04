/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DEFAULT_PARTITION_MANAGER_HPP
#define DEFAULT_PARTITION_MANAGER_HPP

#include "PartitionManager.hpp"
#include <diaspora/DataDescriptor.hpp>
#include <thallium/bulk_buffer.hpp>
#include <thallium/bulk_buffer_pool.hpp>
#include <abt-io.h>
#include <cstdint>
#include <span>
#include <string>
#include <deque>
#include <memory>
#include <algorithm>

namespace mofka {

struct DefaultPartitionManagerOptions {
    std::string        path;
    size_t             max_chunk_size             = 64 * 1024 * 1024;
    size_t             max_events_per_chunk       = 1000000;
    bool               sync                       = true;
    abt_io_instance_id abt_io                     = ABT_IO_INSTANCE_NULL;

    size_t             metadata_pool_num_tiers    = 1;
    size_t             metadata_pool_num_buffers  = 0;
    size_t             metadata_pool_first_size   = 64 * 1024;
    float              metadata_pool_size_multiple = 4.0f;

    size_t             data_pool_num_tiers        = 1;
    size_t             data_pool_num_buffers      = 0;
    size_t             data_pool_first_size       = 64 * 1024 * 1024;
    float              data_pool_size_multiple    = 4.0f;

    size_t             consumer_metadata_pool_num_tiers    = 1;
    size_t             consumer_metadata_pool_num_buffers  = 0;
    size_t             consumer_metadata_pool_first_size   = 64 * 1024;
    float              consumer_metadata_pool_size_multiple = 4.0f;

    size_t             consumer_desc_pool_num_tiers        = 1;
    size_t             consumer_desc_pool_num_buffers      = 0;
    size_t             consumer_desc_pool_first_size       = 4 * 1024;
    float              consumer_desc_pool_size_multiple    = 4.0f;
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

    // ABT-IO
    abt_io_instance_id  m_abt_io;

    // Engine
    thallium::engine    m_engine;

    // Buffer pools for incoming producer RDMA transfers (write_only)
    thallium::bulk_buffer_pool<> m_metadata_buffer_pool;
    thallium::bulk_buffer_pool<> m_data_buffer_pool;

    // Buffer pools for outgoing consumer RDMA transfers (read_only)
    thallium::bulk_buffer_pool<> m_consumer_metadata_buffer_pool;
    thallium::bulk_buffer_pool<> m_consumer_desc_buffer_pool;

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
    size_t                       m_assigned_events = 0; // IDs handed out (may not yet be written)
    size_t                       m_total_events = 0;    // events written and available to consumers
    thallium::mutex              m_events_mtx;
    thallium::condition_variable m_events_cv;

    // Consumer cursors
    std::unordered_map<std::string, diaspora::EventID> m_consumer_cursor;
    thallium::mutex                                    m_consumer_cursor_mtx;

    // Encapsulates the arguments of a receiveBatch call.
    struct PushOperation {

        enum class State : uint8_t {
            submitted,
            assigned,
            metadata_transferred,
            data_transferred,
            stored
        };

        DefaultPartitionManager&     m_manager;
        thallium::request            m_req;
        std::string                  m_producer_name;
        size_t                       m_num_events;
        BulkRef                      m_remote_metadata_bulk;
        BulkRef                      m_remote_data_bulk;
        // Buffers populated by transferMetadata / transferData
        thallium::bulk_buffer<>      m_metadata_buffer;
        std::span<size_t>            m_metadata_sizes;
        std::span<char>              m_metadata_content;
        thallium::bulk_buffer<>      m_data_buffer;
        std::span<size_t>            m_data_sizes;
        std::span<char>              m_data_content;
        diaspora::EventID            m_first_id = 0;
        State                        m_state = State::submitted;
        bool                         m_responded = false;
        thallium::mutex              m_state_mtx;
        thallium::condition_variable m_state_cv;

        PushOperation(DefaultPartitionManager& manager,
                      const thallium::request& req,
                      const std::string& producer_name,
                      size_t num_events,
                      const BulkRef& metadata_bulk,
                      const BulkRef& data_bulk)
        : m_manager(manager)
        , m_req(req)
        , m_producer_name(producer_name)
        , m_num_events(num_events)
        , m_remote_metadata_bulk(metadata_bulk)
        , m_remote_data_bulk(data_bulk)
        {}

        size_t metadataContentSize() const { return m_remote_metadata_bulk.size - m_num_events * sizeof(size_t); }
        size_t dataContentSize()     const { return m_remote_data_bulk.size     - m_num_events * sizeof(size_t); }

        void changeState(State new_state) {
            auto g = std::unique_lock<thallium::mutex>{m_state_mtx};
            if(new_state <= m_state) return;
            m_state = new_state;
            m_state_cv.notify_all();
        }

        void sendResponse(Result<diaspora::EventID> result) {
            if(m_responded) return;
            m_responded = true;
            m_req.respond(result);
        }

        void waitState(State expected) {
            auto g = std::unique_lock<thallium::mutex>{m_state_mtx};
            m_state_cv.wait(g, [this, expected]() { return m_state >= expected; });
        }

        void assignFirstID() {
            m_first_id = m_manager.m_assigned_events;
            m_manager.m_assigned_events += m_num_events;
            changeState(State::assigned);
        }

        void transferMetadata(thallium::engine& engine) {
            (void)engine;
            auto sizes_bytes   = m_num_events * sizeof(size_t);
            auto content_size  = metadataContentSize();
            auto total_size    = sizes_bytes + std::max(content_size, (size_t)1);
            m_metadata_buffer  = m_manager.m_metadata_buffer_pool.get(total_size, /*extend_if_needed=*/true);
            m_metadata_sizes   = std::span<size_t>{
                reinterpret_cast<size_t*>(m_metadata_buffer.data()), m_num_events};
            m_metadata_content = std::span<char>{
                static_cast<char*>(m_metadata_buffer.data()) + sizes_bytes,
                std::max(content_size, (size_t)1)};
            m_metadata_buffer << m_remote_metadata_bulk.handle.on(m_req.get_endpoint()).select(
                m_remote_metadata_bulk.offset, m_remote_metadata_bulk.size);
            m_metadata_content = m_metadata_content.first(content_size);
            changeState(State::metadata_transferred);
        }

        void transferData(thallium::engine& engine) {
            (void)engine;
            auto sizes_bytes  = m_num_events * sizeof(size_t);
            auto content_size = dataContentSize();
            auto total_size   = sizes_bytes + std::max(content_size, (size_t)1);
            m_data_buffer     = m_manager.m_data_buffer_pool.get(total_size, /*extend_if_needed=*/true);
            m_data_sizes      = std::span<size_t>{
                reinterpret_cast<size_t*>(m_data_buffer.data()), m_num_events};
            m_data_content    = std::span<char>{
                static_cast<char*>(m_data_buffer.data()) + sizes_bytes,
                std::max(content_size, (size_t)1)};
            m_data_buffer << m_remote_data_bulk.handle.on(m_req.get_endpoint()).select(
                m_remote_data_bulk.offset, m_remote_data_bulk.size);
            m_data_content = m_data_content.first(content_size);
            changeState(State::data_transferred);
        }

        void writeToFiles();
    };

    // Write queue
    std::deque<std::shared_ptr<PushOperation>> m_write_queue;
    thallium::mutex                            m_write_queue_mtx;
    thallium::condition_variable               m_write_queue_cv;
    bool                                       m_stop = false;
    thallium::managed<thallium::thread>        m_write_ult;

    void writeLoop();

    // RAII handle for a batch of in-flight abt_io_pread_nb operations.
    // Owns the open file descriptors that must stay alive until all ops complete.
    struct PendingReads {
        abt_io_instance_id        m_abt_io = ABT_IO_INSTANCE_NULL;
        std::vector<abt_io_op_t*> m_ops;
        std::vector<ssize_t>      m_rets;     // stable pointers after reserve()
        std::vector<int>          m_open_fds;

        PendingReads() = default;
        explicit PendingReads(abt_io_instance_id ai) : m_abt_io(ai) {}
        PendingReads(PendingReads&&) = default;
        PendingReads& operator=(PendingReads&&) = default;
        PendingReads(const PendingReads&) = delete;
        PendingReads& operator=(const PendingReads&) = delete;
        ~PendingReads() noexcept { wait(); }

        void wait() {
            for(auto* op : m_ops) { abt_io_op_wait(op); abt_io_op_free(op); }
            m_ops.clear();
            m_rets.clear();
            for(int fd : m_open_fds) abt_io_close(m_abt_io, fd);
            m_open_fds.clear();
        }
    };

    // Helpers
    std::string chunkPath(uint32_t chunk_id, const std::string& ext) const;
    void openChunk(uint32_t chunk_id);
    void closeCurrentChunk();
    void rotateChunk();
    bool shouldRotate() const;

    PendingReads readMetadataFromDisk(diaspora::EventID first_id, size_t count,
                                       size_t* sizes_out, char* content_out);
    PendingReads readDescriptorsFromDisk(diaspora::EventID first_id, size_t count,
                                          size_t* sizes_out, char* content_out);
    void readDataFromDisk(const std::vector<diaspora::DataDescriptor>& descriptors,
                          char* buffer, size_t total_size,
                          std::vector<Result<void>>& results);

    public:

    DefaultPartitionManager(thallium::engine engine,
                             DefaultPartitionManagerOptions opts);

    DefaultPartitionManager(DefaultPartitionManager&&) = delete;
    DefaultPartitionManager(const DefaultPartitionManager&) = delete;
    DefaultPartitionManager& operator=(DefaultPartitionManager&&) = delete;
    DefaultPartitionManager& operator=(const DefaultPartitionManager&) = delete;

    virtual ~DefaultPartitionManager();

    void receiveBatch(
            const thallium::request& req,
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
