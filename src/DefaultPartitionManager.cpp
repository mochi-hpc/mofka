/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "DefaultPartitionManager.hpp"
#include <diaspora/DataDescriptor.hpp>
#include <diaspora/BufferWrapperArchive.hpp>
#include <spdlog/spdlog.h>
#include <numeric>
#include <iostream>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

namespace mofka {

MOFKA_REGISTER_PARTITION_MANAGER_WITH_DEPENDENCIES(
    default, DefaultPartitionManager,
    {"abt_io", "abt_io", true, false, false});

static void mkdirs(const std::string& path) {
    std::string current;
    for(size_t i = 0; i < path.size(); ++i) {
        current += path[i];
        if(path[i] == '/' || i == path.size() - 1) {
            mkdir(current.c_str(), 0755);
        }
    }
}

DefaultPartitionManager::DefaultPartitionManager(
        thallium::engine engine,
        DefaultPartitionManagerOptions opts)
: m_path(std::move(opts.path))
, m_max_chunk_size(opts.max_chunk_size)
, m_max_events_per_chunk(opts.max_events_per_chunk)
, m_sync(opts.sync)
, m_abt_io(opts.abt_io)
, m_fd_cache(opts.abt_io, opts.fd_cache_capacity)
, m_engine(std::move(engine))
, m_metadata_buffer_pool(m_engine,
                          opts.metadata_pool_num_tiers,
                          opts.metadata_pool_num_buffers,
                          opts.metadata_pool_first_size,
                          opts.metadata_pool_size_multiple,
                          thallium::bulk_mode::write_only)
, m_data_buffer_pool    (m_engine,
                          opts.data_pool_num_tiers,
                          opts.data_pool_num_buffers,
                          opts.data_pool_first_size,
                          opts.data_pool_size_multiple,
                          thallium::bulk_mode::write_only)
, m_consumer_metadata_buffer_pool(m_engine,
                          opts.consumer_metadata_pool_num_tiers,
                          opts.consumer_metadata_pool_num_buffers,
                          opts.consumer_metadata_pool_first_size,
                          opts.consumer_metadata_pool_size_multiple,
                          thallium::bulk_mode::read_only)
, m_consumer_desc_buffer_pool(m_engine,
                          opts.consumer_desc_pool_num_tiers,
                          opts.consumer_desc_pool_num_buffers,
                          opts.consumer_desc_pool_first_size,
                          opts.consumer_desc_pool_size_multiple,
                          thallium::bulk_mode::read_only)
{
    m_write_ult = m_engine.get_handler_pool().make_thread([this]() { writeLoop(); });
}

std::string DefaultPartitionManager::chunkPath(uint32_t chunk_id, const std::string& ext) const {
    char buf[32];
    snprintf(buf, sizeof(buf), "chunk-%06u", chunk_id);
    return m_path + "/" + buf + "." + ext;
}

void DefaultPartitionManager::openChunk(uint32_t chunk_id) {
    auto open_file = [this](const std::string& path) -> int {
        int fd = abt_io_open(m_abt_io, path.c_str(), O_CREAT | O_RDWR, 0644);
        if(fd < 0) {
            throw diaspora::Exception{
                fmt::format("Failed to open file {}: {}", path, strerror(-fd))};
        }
        return fd;
    };
    m_fd_meta = open_file(chunkPath(chunk_id, "meta"));
    m_fd_data = open_file(chunkPath(chunk_id, "data"));
    m_fd_desc = open_file(chunkPath(chunk_id, "desc"));
    m_fd_idx  = open_file(chunkPath(chunk_id, "idx"));
}

void DefaultPartitionManager::closeCurrentChunk() {
    // Use POSIX close() instead of abt_io_close() because this may be called
    // during destructor teardown when ABT pools are already destroyed.
    // All data has been flushed via abt_io_fdatasync() during normal operation.
    if(m_fd_meta >= 0) { ::close(m_fd_meta); m_fd_meta = -1; }
    if(m_fd_data >= 0) { ::close(m_fd_data); m_fd_data = -1; }
    if(m_fd_desc >= 0) { ::close(m_fd_desc); m_fd_desc = -1; }
    if(m_fd_idx  >= 0) { ::close(m_fd_idx);  m_fd_idx  = -1; }
}

void DefaultPartitionManager::rotateChunk() {
    closeCurrentChunk();
    m_current_chunk_id++;
    m_meta_offset = 0;
    m_data_offset = 0;
    m_desc_offset = 0;
    m_events_in_current_chunk = 0;
    openChunk(m_current_chunk_id);
}

bool DefaultPartitionManager::shouldRotate() const {
    if(m_events_in_current_chunk >= m_max_events_per_chunk)
        return true;
    if((m_meta_offset + m_data_offset) >= m_max_chunk_size)
        return true;
    return false;
}

DefaultPartitionManager::~DefaultPartitionManager() {
    {
        auto g = std::unique_lock<thallium::mutex>{m_write_queue_mtx};
        m_stop = true;
        m_write_queue_cv.notify_all();
    }
    m_write_ult->join();
    closeCurrentChunk();
}

void DefaultPartitionManager::writeLoop() {
    while(true) {
        std::shared_ptr<PushOperation> op;
        {
            auto g = std::unique_lock<thallium::mutex>{m_write_queue_mtx};
            m_write_queue_cv.wait(g, [this]() {
                return !m_write_queue.empty() || m_stop;
            });
            if(m_write_queue.empty()) break;
            op = std::move(m_write_queue.front());
            m_write_queue.pop_front();
        }
        op->waitState(PushOperation::State::data_transferred);
        Result<diaspora::EventID> result;
        try {
            op->writeToFiles();
            result.value() = op->m_first_id;
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
        }
        op->sendResponse(result);
        m_events_cv.notify_all();
    }
}

void DefaultPartitionManager::PushOperation::writeToFiles()
{
    auto& mgr = m_manager;

    std::vector<IndexRecord> records(m_num_events);
    std::vector<char>        desc_buf;

    uint64_t meta_off = mgr.m_meta_offset;
    uint64_t data_off = mgr.m_data_offset;

    {
        diaspora::BufferWrapperOutputArchive output_archive{desc_buf};
        for(size_t i = 0; i < m_num_events; ++i) {
            records[i].metadata_offset  = meta_off;
            records[i].metadata_size    = static_cast<uint32_t>(m_metadata_sizes[i]);
            records[i].data_offset      = data_off;
            records[i].data_size        = static_cast<uint32_t>(m_data_sizes[i]);
            records[i].data_desc_offset = mgr.m_desc_offset;

            FileDataDescriptor fdd;
            fdd.chunk_id = mgr.m_current_chunk_id;
            fdd.offset   = data_off;
            fdd.size     = static_cast<uint32_t>(m_data_sizes[i]);

            auto data_descriptor = diaspora::DataDescriptor(fdd.toString(), fdd.size);
            size_t desc_before = desc_buf.size();
            data_descriptor.save(output_archive);
            size_t desc_size = desc_buf.size() - desc_before;

            records[i].data_desc_size = static_cast<uint32_t>(desc_size);

            meta_off += m_metadata_sizes[i];
            data_off += m_data_sizes[i];
            mgr.m_desc_offset += desc_size;
        }
    }

    // Write metadata to .meta
    if(!m_metadata_content.empty()) {
        ssize_t ret = abt_io_pwrite(mgr.m_abt_io, mgr.m_fd_meta,
            m_metadata_content.data(), m_metadata_content.size(), mgr.m_meta_offset);
        if(ret < 0)
            throw diaspora::Exception{fmt::format("Failed to write metadata: {}", strerror(-ret))};
    }

    // Write data to .data
    if(!m_data_content.empty()) {
        ssize_t ret = abt_io_pwrite(mgr.m_abt_io, mgr.m_fd_data,
            m_data_content.data(), m_data_content.size(), mgr.m_data_offset);
        if(ret < 0)
            throw diaspora::Exception{fmt::format("Failed to write data: {}", strerror(-ret))};
    }

    // Write descriptors to .desc
    if(!desc_buf.empty()) {
        ssize_t ret = abt_io_pwrite(mgr.m_abt_io, mgr.m_fd_desc,
            desc_buf.data(), desc_buf.size(),
            mgr.m_desc_offset - desc_buf.size());
        if(ret < 0)
            throw diaspora::Exception{fmt::format("Failed to write descriptors: {}", strerror(-ret))};
    }

    // Write index records to .idx
    {
        ssize_t ret = abt_io_pwrite(mgr.m_abt_io, mgr.m_fd_idx,
            records.data(),
            m_num_events * sizeof(IndexRecord),
            mgr.m_events_in_current_chunk * sizeof(IndexRecord));
        if(ret < 0)
            throw diaspora::Exception{fmt::format("Failed to write index: {}", strerror(-ret))};
    }

    // Sync if configured
    if(mgr.m_sync) {
        abt_io_fdatasync(mgr.m_abt_io, mgr.m_fd_meta);
        abt_io_fdatasync(mgr.m_abt_io, mgr.m_fd_data);
        abt_io_fdatasync(mgr.m_abt_io, mgr.m_fd_desc);
        abt_io_fdatasync(mgr.m_abt_io, mgr.m_fd_idx);
    }

    // Update in-memory state
    mgr.m_meta_offset = meta_off;
    mgr.m_data_offset = data_off;
    mgr.m_events_in_current_chunk += m_num_events;

    {
        auto g = std::unique_lock<thallium::mutex>{mgr.m_index_mtx};
        for(size_t i = 0; i < m_num_events; ++i) {
            mgr.m_index.push_back(records[i]);
            mgr.m_event_chunk_ids.push_back(mgr.m_current_chunk_id);
        }
    }
    {
        auto g = std::unique_lock<thallium::mutex>{mgr.m_events_mtx};
        mgr.m_total_events += m_num_events;
    }

    if(mgr.shouldRotate())
        mgr.rotateChunk();

    changeState(State::stored);
}

DefaultPartitionManager::PendingReads DefaultPartitionManager::readMetadataFromDisk(
        diaspora::EventID first_id, size_t count,
        size_t* sizes_out, char* content_out) {
    PendingReads pending{m_abt_io};
    pending.m_ops.reserve(count);
    pending.m_rets.reserve(count);
    size_t buf_offset = 0;
    FDCache::EntryPtr current_entry;
    uint32_t current_chunk = UINT32_MAX;
    for(size_t i = 0; i < count; ++i) {
        auto& rec = m_index[first_id + i];
        sizes_out[i] = rec.metadata_size;
        auto chunk_id = m_event_chunk_ids[first_id + i];
        if(chunk_id != current_chunk) {
            current_entry = m_fd_cache.get(chunkPath(chunk_id, "meta"));
            pending.m_entries.push_back(current_entry);
            current_chunk = chunk_id;
        }
        if(current_entry && current_entry->fd >= 0) {
            pending.m_rets.push_back(0);
            pending.m_ops.push_back(
                abt_io_pread_nb(m_abt_io, current_entry->fd, content_out + buf_offset,
                                rec.metadata_size, rec.metadata_offset,
                                &pending.m_rets.back()));
        }
        buf_offset += rec.metadata_size;
    }
    return pending;
}

DefaultPartitionManager::PendingReads DefaultPartitionManager::readDescriptorsFromDisk(
        diaspora::EventID first_id, size_t count,
        size_t* sizes_out, char* content_out) {
    PendingReads pending{m_abt_io};
    pending.m_ops.reserve(count);
    pending.m_rets.reserve(count);
    size_t buf_offset = 0;
    FDCache::EntryPtr current_entry;
    uint32_t current_chunk = UINT32_MAX;
    for(size_t i = 0; i < count; ++i) {
        auto& rec = m_index[first_id + i];
        sizes_out[i] = rec.data_desc_size;
        auto chunk_id = m_event_chunk_ids[first_id + i];
        if(chunk_id != current_chunk) {
            current_entry = m_fd_cache.get(chunkPath(chunk_id, "desc"));
            pending.m_entries.push_back(current_entry);
            current_chunk = chunk_id;
        }
        if(current_entry && current_entry->fd >= 0) {
            pending.m_rets.push_back(0);
            pending.m_ops.push_back(
                abt_io_pread_nb(m_abt_io, current_entry->fd, content_out + buf_offset,
                                rec.data_desc_size, rec.data_desc_offset,
                                &pending.m_rets.back()));
        }
        buf_offset += rec.data_desc_size;
    }
    return pending;
}

void DefaultPartitionManager::readDataFromDisk(
        const std::vector<diaspora::DataDescriptor>& descriptors,
        char* buffer, size_t total_size,
        std::vector<Result<void>>& results) {
    (void)total_size;
    size_t buffer_cursor = 0;
    int current_fd = -1;
    uint32_t current_chunk = UINT32_MAX;
    for(size_t i = 0; i < descriptors.size(); ++i) {
        auto& desc = descriptors[i];
        if(desc.size() == 0) continue;
        FileDataDescriptor fdd = FileDataDescriptor::fromDataDescriptor(desc);
        if(fdd.chunk_id != current_chunk) {
            if(current_fd >= 0) abt_io_close(m_abt_io, current_fd);
            auto path = chunkPath(fdd.chunk_id, "data");
            current_fd = abt_io_open(m_abt_io, path.c_str(), O_RDONLY, 0);
            current_chunk = fdd.chunk_id;
        }
        if(current_fd < 0) {
            results[i].success() = false;
            results[i].error() = fmt::format("Failed to open chunk {}", fdd.chunk_id);
            buffer_cursor += fdd.size;
            continue;
        }
        abt_io_pread(m_abt_io, current_fd, buffer + buffer_cursor,
                     fdd.size, fdd.offset);
        buffer_cursor += fdd.size;
    }
    if(current_fd >= 0) abt_io_close(m_abt_io, current_fd);
}

void DefaultPartitionManager::receiveBatch(
          const thallium::request& req,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    auto op = std::make_shared<PushOperation>(
        *this, req, producer_name, num_events, metadata_bulk, data_bulk);

    {
        auto g = std::unique_lock<thallium::mutex>{m_write_queue_mtx};
        op->assignFirstID();
        m_write_queue.push_back(op);
        m_write_queue_cv.notify_one();
    }

    op->transferMetadata(m_engine);
    op->transferData(m_engine);
}

void DefaultPartitionManager::wakeUp() {
    m_events_cv.notify_all();
}

Result<void> DefaultPartitionManager::feedConsumer(
    ConsumerHandle consumerHandle,
    diaspora::BatchSize batchSize) {
    Result<void> result;

    if(batchSize.value == 0)
        batchSize = diaspora::BatchSize::Adaptive();

    diaspora::EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
        first_id = m_consumer_cursor[consumerHandle.name()];
    }

    auto self_addr = static_cast<std::string>(m_engine.self());

    diaspora::Future<void>   prev_future;
    thallium::bulk_buffer<>  prev_meta_buf, prev_desc_buf;

    while(!consumerHandle.shouldStop()) {
        size_t num_events = 0, total_meta = 0, total_desc = 0;
        thallium::bulk_buffer<> meta_buf, desc_buf;
        PendingReads meta_pending, desc_pending;

        // CS 1: wait for events — only m_total_events access needs m_events_mtx
        bool stop_with_no_events = false;
        {
            auto g = std::unique_lock<thallium::mutex>{m_events_mtx};
            while(true) {
                size_t avail = m_total_events - first_id;
                num_events   = std::min(batchSize.value, avail);
                if(num_events || consumerHandle.shouldStop()) break;
                m_events_cv.wait(g);
            }
            stop_with_no_events = consumerHandle.shouldStop() && num_events == 0;
        }
        if(stop_with_no_events) {
            if(prev_future) prev_future.wait(-1);
            consumerHandle.feed(
                0, diaspora::NoMoreEvents, BulkRef{}, BulkRef{}, BulkRef{}, BulkRef{});
            return result;
        }

        // CS 2: only m_index accesses need m_index_mtx; buffer alloc sits outside
        {
            auto g = std::unique_lock<thallium::mutex>{m_index_mtx};
            for(size_t i = 0; i < num_events; ++i) {
                total_meta += m_index[first_id + i].metadata_size;
                total_desc += m_index[first_id + i].data_desc_size;
            }
        }
        auto sz = num_events * sizeof(size_t);
        meta_buf = m_consumer_metadata_buffer_pool.get(
            sz + std::max(total_meta, (size_t)1), /*extend=*/true);
        desc_buf = m_consumer_desc_buffer_pool.get(
            sz + std::max(total_desc, (size_t)1), /*extend=*/true);
        {
            auto g = std::unique_lock<thallium::mutex>{m_index_mtx};
            meta_pending = readMetadataFromDisk(first_id, num_events,
                reinterpret_cast<size_t*>(meta_buf.data()),
                static_cast<char*>(meta_buf.data()) + sz);
            desc_pending = readDescriptorsFromDisk(first_id, num_events,
                reinterpret_cast<size_t*>(desc_buf.data()),
                static_cast<char*>(desc_buf.data()) + sz);
        }

        // No mutex: wait disk reads, drain previous RDMA, start new RDMA
        meta_pending.wait();
        desc_pending.wait();

        if(prev_future) { prev_future.wait(-1); prev_future = {}; }
        prev_meta_buf = {};
        prev_desc_buf = {};

        prev_future = consumerHandle.feed(
            num_events, first_id,
            BulkRef{meta_buf.bulk(), 0,  sz,          self_addr},
            BulkRef{meta_buf.bulk(), sz, total_meta,   self_addr},
            BulkRef{desc_buf.bulk(), 0,  sz,          self_addr},
            BulkRef{desc_buf.bulk(), sz, total_desc,   self_addr});
        prev_meta_buf = std::move(meta_buf);
        prev_desc_buf = std::move(desc_buf);
        first_id     += num_events;
    }

    if(prev_future) prev_future.wait(-1);
    return result;
}

Result<void> DefaultPartitionManager::acknowledge(
    std::string_view consumer_name,
    diaspora::EventID event_id) {
    Result<void> result;
    auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
    std::string consumer_name_str{consumer_name.data(), consumer_name.size()};
    m_consumer_cursor[consumer_name_str] = event_id + 1;
    return result;
}

Result<std::vector<Result<void>>> DefaultPartitionManager::getData(
        const std::vector<diaspora::DataDescriptor>& descriptors,
        const BulkRef& bulk) {
    Result<std::vector<Result<void>>> result;
    result.value().resize(descriptors.size());

    auto client_address = m_engine.lookup(bulk.address);

    // Calculate total size from the file descriptors embedded in each DataDescriptor
    size_t total_data_size = 0;
    for(auto& desc : descriptors) {
        if(desc.size() == 0) continue;
        total_data_size += FileDataDescriptor::fromDataDescriptor(desc).size;
    }

    // Read all data from disk into a flat buffer
    std::vector<char> data_buffer(total_data_size);
    readDataFromDisk(descriptors, data_buffer.data(), total_data_size, result.value());

    // Build segments for the bulk transfer, respecting each descriptor's flatten() layout
    std::vector<std::pair<void*, size_t>> local_segments;
    size_t buffer_cursor = 0;
    for(size_t i = 0; i < descriptors.size(); ++i) {
        auto& desc = descriptors[i];
        if(desc.size() == 0) continue;
        FileDataDescriptor fdd = FileDataDescriptor::fromDataDescriptor(desc);
        if(result.value()[i].success()) {
            for(auto& seg : desc.flatten()) {
                local_segments.push_back({
                    data_buffer.data() + buffer_cursor + seg.offset,
                    seg.size
                });
            }
        }
        buffer_cursor += fdd.size;
    }

    if(!local_segments.empty()) {
        auto local_data_bulk = m_engine.expose(
            local_segments, thallium::bulk_mode::read_only);
        bulk.handle.on(client_address) << local_data_bulk;
    }

    return result;
}

Result<bool> DefaultPartitionManager::destroy() {
    Result<bool> result;
    result.value() = true;
    return result;
}

std::unique_ptr<mofka::PartitionManager> DefaultPartitionManager::create(
        const thallium::engine& engine,
        const std::string& topic_name,
        const UUID& partition_uuid,
        const diaspora::Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies) {

    static const nlohmann::json configSchema = R"(
    {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "type": "object",
        "properties":{
            "path": {"type": "string"},
            "max_chunk_size": {"type": "integer"},
            "max_events_per_chunk": {"type": "integer"},
            "sync": {"type": "boolean"},
            "fd_cache_capacity": {"type": "integer", "minimum": 1},
            "producers": {
                "type": "object",
                "properties": {
                    "metadata_buffer_pool": {
                        "type": "object",
                        "properties": {
                            "num_tiers":     {"type": "integer", "minimum": 1},
                            "num_buffers":   {"type": "integer", "minimum": 0},
                            "first_size":    {"type": "integer", "minimum": 1},
                            "size_multiple": {"type": "number",  "exclusiveMinimum": 1.0}
                        }
                    },
                    "data_buffer_pool": {
                        "type": "object",
                        "properties": {
                            "num_tiers":     {"type": "integer", "minimum": 1},
                            "num_buffers":   {"type": "integer", "minimum": 0},
                            "first_size":    {"type": "integer", "minimum": 1},
                            "size_multiple": {"type": "number",  "exclusiveMinimum": 1.0}
                        }
                    }
                }
            },
            "consumers": {
                "type": "object",
                "properties": {
                    "metadata_buffer_pool": {
                        "type": "object",
                        "properties": {
                            "num_tiers":     {"type": "integer", "minimum": 1},
                            "num_buffers":   {"type": "integer", "minimum": 0},
                            "first_size":    {"type": "integer", "minimum": 1},
                            "size_multiple": {"type": "number",  "exclusiveMinimum": 1.0}
                        }
                    },
                    "desc_buffer_pool": {
                        "type": "object",
                        "properties": {
                            "num_tiers":     {"type": "integer", "minimum": 1},
                            "num_buffers":   {"type": "integer", "minimum": 0},
                            "first_size":    {"type": "integer", "minimum": 1},
                            "size_multiple": {"type": "number",  "exclusiveMinimum": 1.0}
                        }
                    }
                }
            }
        },
        "required": ["path"]
    }
    )"_json;

    /* Validate configuration against schema */
    static JsonSchemaValidator schemaValidator{configSchema};
    auto validationErrors = schemaValidator.validate(config.json());
    if(!validationErrors.empty()) {
        spdlog::error("[mofka] Error(s) while validating JSON config for DefaultPartitionManager:");
        for(auto& error : validationErrors) spdlog::error("[mofka] \t{}", error);
        throw diaspora::Exception{
            "Error(s) while validating JSON config for DefaultPartitionManager"};
    }

    /* Extract ABT-IO dependency */
    auto abt_io_component = dependencies.at("abt_io")[0]->getHandle<bedrock::ComponentPtr>();
    auto abt_io = static_cast<abt_io_instance_id>(abt_io_component->getHandle());

    /* Parse config */
    auto& json = config.json();
    std::string base_path        = json["path"].get<std::string>();
    size_t max_chunk_size        = json.value("max_chunk_size", (size_t)(64 * 1024 * 1024));
    size_t max_events_per_chunk  = json.value("max_events_per_chunk", (size_t)1000000);
    bool sync                    = json.value("sync", true);
    size_t fd_cache_capacity     = json.value("fd_cache_capacity", (size_t)64);

    size_t meta_num_tiers     = json.value("/producers/metadata_buffer_pool/num_tiers"_json_pointer,     (size_t)1);
    size_t meta_num_buffers   = json.value("/producers/metadata_buffer_pool/num_buffers"_json_pointer,   (size_t)0);
    size_t meta_first_size    = json.value("/producers/metadata_buffer_pool/first_size"_json_pointer,    (size_t)(64*1024));
    float  meta_size_multiple = json.value("/producers/metadata_buffer_pool/size_multiple"_json_pointer, 4.0f);

    size_t data_num_tiers     = json.value("/producers/data_buffer_pool/num_tiers"_json_pointer,         (size_t)1);
    size_t data_num_buffers   = json.value("/producers/data_buffer_pool/num_buffers"_json_pointer,       (size_t)0);
    size_t data_first_size    = json.value("/producers/data_buffer_pool/first_size"_json_pointer,        (size_t)(64*1024*1024));
    float  data_size_multiple = json.value("/producers/data_buffer_pool/size_multiple"_json_pointer,     4.0f);

    size_t cmeta_num_tiers     = json.value("/consumers/metadata_buffer_pool/num_tiers"_json_pointer,     (size_t)1);
    size_t cmeta_num_buffers   = json.value("/consumers/metadata_buffer_pool/num_buffers"_json_pointer,   (size_t)0);
    size_t cmeta_first_size    = json.value("/consumers/metadata_buffer_pool/first_size"_json_pointer,    (size_t)(64*1024));
    float  cmeta_size_multiple = json.value("/consumers/metadata_buffer_pool/size_multiple"_json_pointer, 4.0f);

    size_t cdesc_num_tiers     = json.value("/consumers/desc_buffer_pool/num_tiers"_json_pointer,         (size_t)1);
    size_t cdesc_num_buffers   = json.value("/consumers/desc_buffer_pool/num_buffers"_json_pointer,       (size_t)0);
    size_t cdesc_first_size    = json.value("/consumers/desc_buffer_pool/first_size"_json_pointer,        (size_t)(4*1024));
    float  cdesc_size_multiple = json.value("/consumers/desc_buffer_pool/size_multiple"_json_pointer,     4.0f);

    /* Create directory: <path>/<topic_name>-<uuid>/ */
    std::string partition_path = base_path + "/" + topic_name + "-" + partition_uuid.to_string();
    mkdirs(partition_path);

    /* Scan for existing chunk files to recover state */
    uint32_t current_chunk_id = 0;
    size_t total_events = 0;
    std::vector<DefaultPartitionManager::IndexRecord> index;
    std::vector<uint32_t> event_chunk_ids;
    uint64_t meta_offset = 0;
    uint64_t data_offset = 0;
    uint64_t desc_offset = 0;
    size_t events_in_current_chunk = 0;

    while(true) {
        char buf[32];
        snprintf(buf, sizeof(buf), "chunk-%06u", current_chunk_id);
        std::string idx_path = partition_path + "/" + buf + ".idx";

        struct stat st;
        if(stat(idx_path.c_str(), &st) != 0) break;

        size_t num_records = st.st_size / sizeof(IndexRecord);
        if(num_records == 0) break;

        std::vector<IndexRecord> chunk_records(num_records);
        int fd = open(idx_path.c_str(), O_RDONLY);
        if(fd < 0) break;
        (void)read(fd, chunk_records.data(), num_records * sizeof(IndexRecord));
        close(fd);

        for(size_t i = 0; i < num_records; ++i) {
            index.push_back(chunk_records[i]);
            event_chunk_ids.push_back(current_chunk_id);
        }

        events_in_current_chunk = num_records;
        total_events += num_records;

        auto& last = chunk_records.back();
        meta_offset = last.metadata_offset + last.metadata_size;
        data_offset = last.data_offset + last.data_size;
        desc_offset = last.data_desc_offset + last.data_desc_size;

        char next_buf[32];
        snprintf(next_buf, sizeof(next_buf), "chunk-%06u", current_chunk_id + 1);
        std::string next_idx_path = partition_path + "/" + next_buf + ".idx";
        struct stat next_st;
        if(stat(next_idx_path.c_str(), &next_st) == 0) {
            current_chunk_id++;
            events_in_current_chunk = 0;
            meta_offset = 0;
            data_offset = 0;
            desc_offset = 0;
        } else {
            break;
        }
    }

    /* Create the manager */
    auto manager = std::unique_ptr<DefaultPartitionManager>(
        new DefaultPartitionManager(engine, DefaultPartitionManagerOptions{
            .path                       = partition_path,
            .max_chunk_size             = max_chunk_size,
            .max_events_per_chunk       = max_events_per_chunk,
            .sync                       = sync,
            .abt_io                     = abt_io,
            .metadata_pool_num_tiers    = meta_num_tiers,
            .metadata_pool_num_buffers  = meta_num_buffers,
            .metadata_pool_first_size   = meta_first_size,
            .metadata_pool_size_multiple = meta_size_multiple,
            .data_pool_num_tiers        = data_num_tiers,
            .data_pool_num_buffers      = data_num_buffers,
            .data_pool_first_size       = data_first_size,
            .data_pool_size_multiple    = data_size_multiple,
            .consumer_metadata_pool_num_tiers     = cmeta_num_tiers,
            .consumer_metadata_pool_num_buffers   = cmeta_num_buffers,
            .consumer_metadata_pool_first_size    = cmeta_first_size,
            .consumer_metadata_pool_size_multiple = cmeta_size_multiple,
            .consumer_desc_pool_num_tiers         = cdesc_num_tiers,
            .consumer_desc_pool_num_buffers       = cdesc_num_buffers,
            .consumer_desc_pool_first_size        = cdesc_first_size,
            .consumer_desc_pool_size_multiple     = cdesc_size_multiple,
            .fd_cache_capacity                    = fd_cache_capacity,
        }));

    manager->m_current_chunk_id = current_chunk_id;
    manager->m_assigned_events = total_events;
    manager->m_total_events = total_events;
    manager->m_index = std::move(index);
    manager->m_event_chunk_ids = std::move(event_chunk_ids);
    manager->m_meta_offset = meta_offset;
    manager->m_data_offset = data_offset;
    manager->m_desc_offset = desc_offset;
    manager->m_events_in_current_chunk = events_in_current_chunk;

    /* Open current chunk files */
    manager->openChunk(current_chunk_id);

    return manager;
}

}
