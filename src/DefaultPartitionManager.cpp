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
    if(m_ack_early) {
        {
            auto g = std::unique_lock<thallium::mutex>{m_pending_writes_mtx};
            m_writer_stop = true;
        }
        m_pending_writes_ready_cv.notify_all();
        m_writer_done.wait();
    }
    closeCurrentChunk();
}

DefaultPartitionManager::WriteBatchResult DefaultPartitionManager::writeBatchToFiles(
        size_t num_events,
        const size_t* metadata_sizes, const char* metadata_content, size_t metadata_content_size,
        const size_t* data_sizes, const char* data_content, size_t data_content_size)
{
    WriteBatchResult wb;
    wb.chunk_id = m_current_chunk_id;
    wb.records.resize(num_events);
    wb.desc_sizes.resize(num_events);

    uint64_t meta_off = m_meta_offset;
    uint64_t data_off = m_data_offset;

    {
        diaspora::BufferWrapperOutputArchive output_archive{wb.desc_buf};
        for(size_t i = 0; i < num_events; ++i) {
            wb.records[i].metadata_offset = meta_off;
            wb.records[i].metadata_size   = static_cast<uint32_t>(metadata_sizes[i]);
            wb.records[i].data_offset     = data_off;
            wb.records[i].data_size       = static_cast<uint32_t>(data_sizes[i]);
            wb.records[i].data_desc_offset = m_desc_offset;

            FileDataDescriptor fdd;
            fdd.chunk_id = m_current_chunk_id;
            fdd.offset   = data_off;
            fdd.size     = static_cast<uint32_t>(data_sizes[i]);

            auto data_descriptor = diaspora::DataDescriptor(fdd.toString(), fdd.size);
            size_t desc_buf_before = wb.desc_buf.size();
            data_descriptor.save(output_archive);
            size_t desc_size = wb.desc_buf.size() - desc_buf_before;

            wb.desc_sizes[i] = desc_size;
            wb.records[i].data_desc_size = static_cast<uint32_t>(desc_size);

            meta_off += metadata_sizes[i];
            data_off += data_sizes[i];
            m_desc_offset += desc_size;
        }
    }

    // Write metadata to .meta
    if(metadata_content_size > 0) {
        ssize_t ret = abt_io_pwrite(m_abt_io, m_fd_meta,
            metadata_content, metadata_content_size, m_meta_offset);
        if(ret < 0) {
            wb.success = false;
            wb.error = fmt::format("Failed to write metadata: {}", strerror(-ret));
            return wb;
        }
    }

    // Write data to .data
    if(data_content_size > 0) {
        ssize_t ret = abt_io_pwrite(m_abt_io, m_fd_data,
            data_content, data_content_size, m_data_offset);
        if(ret < 0) {
            wb.success = false;
            wb.error = fmt::format("Failed to write data: {}", strerror(-ret));
            return wb;
        }
    }

    // Write descriptors to .desc
    if(!wb.desc_buf.empty()) {
        ssize_t ret = abt_io_pwrite(m_abt_io, m_fd_desc,
            wb.desc_buf.data(), wb.desc_buf.size(),
            m_desc_offset - wb.desc_buf.size());
        if(ret < 0) {
            wb.success = false;
            wb.error = fmt::format("Failed to write descriptors: {}", strerror(-ret));
            return wb;
        }
    }

    // Write index records to .idx
    {
        ssize_t ret = abt_io_pwrite(m_abt_io, m_fd_idx,
            wb.records.data(),
            num_events * sizeof(IndexRecord),
            m_events_in_current_chunk * sizeof(IndexRecord));
        if(ret < 0) {
            wb.success = false;
            wb.error = fmt::format("Failed to write index: {}", strerror(-ret));
            return wb;
        }
    }

    // Sync if configured
    if(m_sync) {
        abt_io_fdatasync(m_abt_io, m_fd_meta);
        abt_io_fdatasync(m_abt_io, m_fd_data);
        abt_io_fdatasync(m_abt_io, m_fd_desc);
        abt_io_fdatasync(m_abt_io, m_fd_idx);
    }

    // Update in-memory state
    m_meta_offset = meta_off;
    m_data_offset = data_off;
    m_events_in_current_chunk += num_events;

    for(size_t i = 0; i < num_events; ++i) {
        m_index.push_back(wb.records[i]);
        m_event_chunk_ids.push_back(m_current_chunk_id);
    }
    m_total_events += num_events;

    // Check if we need to rotate
    if(shouldRotate()) {
        rotateChunk();
    }

    return wb;
}

void DefaultPartitionManager::readMetadataFromDisk(
        diaspora::EventID first_id, size_t count,
        size_t* sizes_out, char* content_out) {
    size_t buf_offset = 0;
    int current_fd = -1;
    uint32_t current_chunk = UINT32_MAX;
    for(size_t i = 0; i < count; ++i) {
        auto& rec = m_index[first_id + i];
        sizes_out[i] = rec.metadata_size;
        auto chunk_id = m_event_chunk_ids[first_id + i];
        if(chunk_id != current_chunk) {
            if(current_fd >= 0) abt_io_close(m_abt_io, current_fd);
            auto path = chunkPath(chunk_id, "meta");
            current_fd = abt_io_open(m_abt_io, path.c_str(), O_RDONLY, 0);
            current_chunk = chunk_id;
        }
        if(current_fd >= 0) {
            abt_io_pread(m_abt_io, current_fd, content_out + buf_offset,
                         rec.metadata_size, rec.metadata_offset);
        }
        buf_offset += rec.metadata_size;
    }
    if(current_fd >= 0) abt_io_close(m_abt_io, current_fd);
}

void DefaultPartitionManager::readDescriptorsFromDisk(
        diaspora::EventID first_id, size_t count,
        size_t* sizes_out, char* content_out) {
    size_t buf_offset = 0;
    int current_fd = -1;
    uint32_t current_chunk = UINT32_MAX;
    for(size_t i = 0; i < count; ++i) {
        auto& rec = m_index[first_id + i];
        sizes_out[i] = rec.data_desc_size;
        auto chunk_id = m_event_chunk_ids[first_id + i];
        if(chunk_id != current_chunk) {
            if(current_fd >= 0) abt_io_close(m_abt_io, current_fd);
            auto path = chunkPath(chunk_id, "desc");
            current_fd = abt_io_open(m_abt_io, path.c_str(), O_RDONLY, 0);
            current_chunk = chunk_id;
        }
        if(current_fd >= 0) {
            abt_io_pread(m_abt_io, current_fd, content_out + buf_offset,
                         rec.data_desc_size, rec.data_desc_offset);
        }
        buf_offset += rec.data_desc_size;
    }
    if(current_fd >= 0) abt_io_close(m_abt_io, current_fd);
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

Result<diaspora::EventID> DefaultPartitionManager::receiveBatch(
          const thallium::endpoint& sender,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    (void)producer_name;
    Result<diaspora::EventID> result;

    auto metadata_content_size = metadata_bulk.size - num_events*sizeof(size_t);
    auto data_content_size = data_bulk.size - num_events*sizeof(size_t);

    diaspora::EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_write_mtx};

        std::vector<size_t> metadata_sizes(num_events);
        std::vector<char>   metadata_content(std::max(metadata_content_size, (size_t)1));
        std::vector<size_t> data_sizes(num_events);
        std::vector<char>   data_content(std::max(data_content_size, (size_t)1));

        auto local_metadata_bulk = m_engine.expose(
            {{(char*)metadata_sizes.data(), num_events*sizeof(size_t)},
             {metadata_content.data(), metadata_content.size()}},
            thallium::bulk_mode::write_only);
        local_metadata_bulk << metadata_bulk.handle.on(sender).select(
            metadata_bulk.offset, metadata_bulk.size);

        auto local_data_bulk = m_engine.expose(
            {{(char*)data_sizes.data(), num_events*sizeof(size_t)},
             {data_content.data(), data_content.size()}},
            thallium::bulk_mode::write_only);
        local_data_bulk << data_bulk.handle.on(sender).select(
            data_bulk.offset, data_bulk.size);

        metadata_content.resize(metadata_content_size);
        data_content.resize(data_content_size);

        first_id = m_total_events;

        auto wb = writeBatchToFiles(
            num_events,
            metadata_sizes.data(), metadata_content.data(), metadata_content_size,
            data_sizes.data(), data_content.data(), data_content_size);

        if(!wb.success) {
            result.success() = false;
            result.error() = std::move(wb.error);
            return result;
        }
    }

    m_events_cv.notify_all();
    result.value() = first_id;
    return result;
}

Result<diaspora::EventID> DefaultPartitionManager::receiveBatchAckEarly(
          const thallium::endpoint& sender,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    (void)producer_name;
    Result<diaspora::EventID> result;

    auto metadata_content_size = metadata_bulk.size - num_events*sizeof(size_t);
    auto data_content_size = data_bulk.size - num_events*sizeof(size_t);

    // Backpressure: wait until the queue has room
    {
        auto g = std::unique_lock<thallium::mutex>{m_pending_writes_mtx};
        m_pending_writes_cv.wait(g, [this]() {
            return m_pending_writes.size() < m_max_pending_batches;
        });
    }

    // RDMA pull into owned vectors before the RPC returns
    PendingWrite pw;
    pw.num_events = num_events;
    pw.metadata_sizes.resize(num_events);
    pw.metadata_content.resize(std::max(metadata_content_size, (size_t)1));
    pw.data_sizes.resize(num_events);
    pw.data_content.resize(std::max(data_content_size, (size_t)1));

    {
        auto local_metadata_bulk = m_engine.expose(
            {{(char*)pw.metadata_sizes.data(), num_events*sizeof(size_t)},
             {pw.metadata_content.data(), pw.metadata_content.size()}},
            thallium::bulk_mode::write_only);
        local_metadata_bulk << metadata_bulk.handle.on(sender).select(
            metadata_bulk.offset, metadata_bulk.size);

        auto local_data_bulk = m_engine.expose(
            {{(char*)pw.data_sizes.data(), num_events*sizeof(size_t)},
             {pw.data_content.data(), pw.data_content.size()}},
            thallium::bulk_mode::write_only);
        local_data_bulk << data_bulk.handle.on(sender).select(
            data_bulk.offset, data_bulk.size);
    }

    pw.metadata_content.resize(metadata_content_size);
    pw.data_content.resize(data_content_size);

    pw.first_id = m_assigned_events.fetch_add(num_events);
    diaspora::EventID first_id = pw.first_id;

    {
        auto g = std::unique_lock<thallium::mutex>{m_pending_writes_mtx};
        m_pending_writes.push(std::move(pw));
    }
    m_pending_writes_ready_cv.notify_one();

    result.value() = first_id;
    return result;
}

void DefaultPartitionManager::processPendingWrite(PendingWrite& pw) {
    auto wb = writeBatchToFiles(
        pw.num_events,
        pw.metadata_sizes.data(), pw.metadata_content.data(), pw.metadata_content.size(),
        pw.data_sizes.data(), pw.data_content.data(), pw.data_content.size());

    if(!wb.success) {
        spdlog::error("[mofka] Background write failed: {}", wb.error);
    }
}

void DefaultPartitionManager::backgroundWriterLoop() {
    while(true) {
        PendingWrite pw;
        {
            auto g = std::unique_lock<thallium::mutex>{m_pending_writes_mtx};
            m_pending_writes_ready_cv.wait(g, [this]() {
                return !m_pending_writes.empty() || m_writer_stop;
            });
            if(m_writer_stop && m_pending_writes.empty()) break;
            pw = std::move(m_pending_writes.front());
            m_pending_writes.pop();
        }
        m_pending_writes_cv.notify_all();

        {
            auto g = std::unique_lock<thallium::mutex>{m_write_mtx};
            processPendingWrite(pw);
        }

        m_events_cv.notify_all();
    }
    m_writer_done.set_value();
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
    {
        auto g = std::unique_lock<thallium::mutex>{m_events_mtx};
        while(!consumerHandle.shouldStop()) {
            size_t num_events_to_send;
            bool should_stop = false;
            while(true) {
                size_t max_available = m_total_events - first_id;
                num_events_to_send = std::min(batchSize.value, max_available);
                should_stop = consumerHandle.shouldStop();
                if(num_events_to_send != 0 || should_stop) break;
                m_events_cv.wait(g);
            }
            if(should_stop) break;

            if(num_events_to_send == 0) {
                consumerHandle.feed(
                    0, diaspora::NoMoreEvents, BulkRef{}, BulkRef{}, BulkRef{}, BulkRef{});
                break;
            }

            // Compute total sizes from in-memory index
            size_t total_metadata_size = 0;
            size_t total_desc_size = 0;
            for(size_t i = 0; i < num_events_to_send; ++i) {
                total_metadata_size += m_index[first_id + i].metadata_size;
                total_desc_size     += m_index[first_id + i].data_desc_size;
            }

            // Allocate per-call staging buffers
            std::vector<size_t> metadata_sizes(num_events_to_send);
            std::vector<char>   metadata_content(std::max(total_metadata_size, (size_t)1));
            std::vector<size_t> desc_sizes(num_events_to_send);
            std::vector<char>   desc_content(std::max(total_desc_size, (size_t)1));

            readMetadataFromDisk(first_id, num_events_to_send,
                                 metadata_sizes.data(), metadata_content.data());
            readDescriptorsFromDisk(first_id, num_events_to_send,
                                    desc_sizes.data(), desc_content.data());

            // Expose as read-only bulk
            auto metadata_bulk = m_engine.expose(
                {{(char*)metadata_sizes.data(), num_events_to_send*sizeof(size_t)},
                 {metadata_content.data(), total_metadata_size}},
                thallium::bulk_mode::read_only);
            auto metadata_size_bulk_ref = BulkRef{
                metadata_bulk, 0, num_events_to_send*sizeof(size_t), self_addr};
            auto metadata_bulk_ref = BulkRef{
                metadata_bulk, num_events_to_send*sizeof(size_t), total_metadata_size, self_addr};

            auto desc_bulk = m_engine.expose(
                {{(char*)desc_sizes.data(), num_events_to_send*sizeof(size_t)},
                 {desc_content.data(), total_desc_size}},
                thallium::bulk_mode::read_only);
            auto data_desc_size_bulk_ref = BulkRef{
                desc_bulk, 0, num_events_to_send*sizeof(size_t), self_addr};
            auto data_desc_bulk_ref = BulkRef{
                desc_bulk, num_events_to_send*sizeof(size_t), total_desc_size, self_addr};

            consumerHandle.feed(
                num_events_to_send,
                first_id,
                metadata_size_bulk_ref,
                metadata_bulk_ref,
                data_desc_size_bulk_ref,
                data_desc_bulk_ref).wait(-1);

            first_id += num_events_to_send;
        }
    }

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
            "ack_early": {
                "type": "object",
                "properties": {
                    "enabled": {"type": "boolean"},
                    "max_pending_batches": {"type": "integer", "minimum": 1}
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
    std::string base_path = json["path"].get<std::string>();
    size_t max_chunk_size = json.value("max_chunk_size", (size_t)(64 * 1024 * 1024));
    size_t max_events_per_chunk = json.value("max_events_per_chunk", (size_t)1000000);
    bool sync = json.value("sync", true);

    /* Parse ack_early config */
    bool ack_early_enabled = false;
    size_t max_pending_batches = 8;
    if(json.contains("ack_early")) {
        auto& ae = json["ack_early"];
        ack_early_enabled = ae.value("enabled", false);
        max_pending_batches = ae.value("max_pending_batches", (size_t)8);
    }

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
        new DefaultPartitionManager(
            partition_path, max_chunk_size, max_events_per_chunk,
            sync, abt_io, engine));

    manager->m_current_chunk_id = current_chunk_id;
    manager->m_total_events = total_events;
    manager->m_assigned_events.store(total_events);
    manager->m_index = std::move(index);
    manager->m_event_chunk_ids = std::move(event_chunk_ids);
    manager->m_meta_offset = meta_offset;
    manager->m_data_offset = data_offset;
    manager->m_desc_offset = desc_offset;
    manager->m_events_in_current_chunk = events_in_current_chunk;
    manager->m_ack_early = ack_early_enabled;
    manager->m_max_pending_batches = max_pending_batches;

    /* Open current chunk files */
    manager->openChunk(current_chunk_id);

    /* Start background writer ULT if ack_early is enabled */
    if(ack_early_enabled) {
        auto mgr = manager.get();
        thallium::pool(engine.get_handler_pool()).make_thread(
            [mgr]() { mgr->backgroundWriterLoop(); },
            thallium::anonymous{});
    }

    return manager;
}

}
