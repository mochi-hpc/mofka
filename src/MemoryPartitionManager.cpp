/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "MemoryPartitionManager.hpp"
#include <diaspora/DataDescriptor.hpp>
#include <diaspora/BufferWrapperArchive.hpp>
#include <numeric>

namespace mofka {

MOFKA_REGISTER_PARTITION_MANAGER(memory, MemoryPartitionManager);

void MemoryPartitionManager::receiveBatch(
          const thallium::request& req,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    auto sender = req.get_endpoint();
    (void)producer_name;
    Result<diaspora::EventID> result;

    // Pull metadata and data into local staging buffers WITHOUT holding any
    // partition lock. Bulk transfers are yielding operations; holding
    // m_events_metadata_mtx or m_events_data_mtx across them blocks
    // concurrent getData / feedConsumer ULTs on the same Argobots pool and
    // can deadlock if their progress is required for the bulk pull to make
    // forward progress on a busy pool.
    auto metadata_size = metadata_bulk.size - num_events*sizeof(size_t);
    auto data_size     = data_bulk.size     - num_events*sizeof(size_t);

    std::vector<size_t> tmp_metadata_sizes(num_events);
    std::vector<char>   tmp_metadata(metadata_size);
    std::vector<size_t> tmp_data_sizes(num_events);
    std::vector<char>   tmp_data(data_size);

    {
        auto local_metadata_bulk = m_engine.expose(
            {{(char*)tmp_metadata_sizes.data(), num_events*sizeof(size_t)},
             {tmp_metadata.data(), metadata_size}},
            thallium::bulk_mode::write_only);
        local_metadata_bulk << metadata_bulk.handle.on(sender).select(
            metadata_bulk.offset, metadata_bulk.size);
    }
    {
        auto local_data_bulk = m_engine.expose(
            {{(char*)tmp_data_sizes.data(), num_events*sizeof(size_t)},
             {tmp_data.data(), data_size}},
            thallium::bulk_mode::write_only);
        local_data_bulk << data_bulk.handle.on(sender).select(
            data_bulk.offset, data_bulk.size);
    }

    diaspora::EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_events_metadata_mtx};
        first_id = m_events_metadata_sizes.size();
        size_t first_metadata_offset = m_events_metadata.size();
        m_events_metadata_sizes.insert(
            m_events_metadata_sizes.end(),
            tmp_metadata_sizes.begin(), tmp_metadata_sizes.end());
        m_events_metadata.insert(
            m_events_metadata.end(),
            tmp_metadata.begin(), tmp_metadata.end());
        m_events_metadata_offsets.resize(first_id + num_events);
        size_t metadata_offset = first_metadata_offset;
        for(size_t i = first_id; i < first_id + num_events; ++i) {
            m_events_metadata_offsets[i] = metadata_offset;
            metadata_offset += m_events_metadata_sizes[i];
        }

        // Data section: take m_events_data_mtx briefly to update the data
        // vectors. We hold both locks here, but only to do in-memory copies
        // — no yielding operations.
        std::unique_lock<thallium::mutex> data_lock{m_events_data_mtx};
        size_t first_data_offset = m_events_data.size();
        m_events_data_sizes.insert(
            m_events_data_sizes.end(),
            tmp_data_sizes.begin(), tmp_data_sizes.end());
        m_events_data.insert(
            m_events_data.end(),
            tmp_data.begin(), tmp_data.end());
        m_events_data_offsets.resize(first_id + num_events);
        size_t data_offset = first_data_offset;
        for(size_t i = first_id; i < first_id + num_events; ++i) {
            m_events_data_offsets[i] = data_offset;
            data_offset += m_events_data_sizes[i];
        }
        // update the DataDescriptor information
        size_t data_desc_offset = 0;
        if(!m_events_data_desc_offsets.empty())
            data_desc_offset = m_events_data_desc_offsets.back() + m_events_data_desc_sizes.back();
        m_events_data_desc_sizes.resize(first_id + num_events);
        m_events_data_desc_offsets.resize(first_id + num_events);
        diaspora::BufferWrapperOutputArchive output_archive{m_events_data_desc};
        for(size_t i = first_id; i < first_id + num_events; ++i) {
            auto offset_size = OffsetSize{m_events_data_offsets[i], m_events_data_sizes[i]};
            auto data_descriptor = diaspora::DataDescriptor(offset_size.toString(), offset_size.size);
            size_t m_events_data_desc_size = m_events_data_desc.size();
            data_descriptor.save(output_archive);
            auto data_descriptor_size = m_events_data_desc.size() - m_events_data_desc_size;
            m_events_data_desc_sizes[i] = data_descriptor_size;
            m_events_data_desc_offsets[i] = data_desc_offset;
            data_desc_offset += data_descriptor_size;
        }
        // Notify under the lock so a feedConsumer ULT waiting on m_events_cv
        // cannot miss the wake-up (the wait re-checks its predicate under the same mutex).
        m_events_cv.notify_all();
    }
    result.value() = first_id;
    req.respond(result);
}

void MemoryPartitionManager::wakeUp() {
    m_events_cv.notify_all();
}

Result<void> MemoryPartitionManager::feedConsumer(
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
        auto g = std::unique_lock<thallium::mutex>{m_events_metadata_mtx};
        while(!consumerHandle.shouldStop()) {
            size_t num_events_to_send;
            bool should_stop = false;
            while(true) {
                // find the number of events we can send
                size_t max_available_events = m_events_metadata_sizes.size() - first_id;
                num_events_to_send = std::min(batchSize.value, max_available_events);
                should_stop = consumerHandle.shouldStop();
                if(num_events_to_send != 0 || should_stop) break;
                m_events_cv.wait(g);
            }
            if(should_stop) break;

            if(num_events_to_send == 0) { // m_is_marked_complete must be true
                // feed consumer 0 events with first_id = NoMoreEvents to indicate
                // that there are no more events to consume from this partition
                consumerHandle.feed(
                        0, diaspora::NoMoreEvents, BulkRef{}, BulkRef{}, BulkRef{}, BulkRef{});
                break;
            }

            // find the range of metadata sizes
            const auto metadata_sizes_ptr = m_events_metadata_sizes.data() + first_id;
            // find the metadata content
            const auto metadata_ptr = m_events_metadata.data() + m_events_metadata_offsets[first_id];
            const auto metadata_size = std::accumulate(
                    metadata_sizes_ptr, metadata_sizes_ptr + num_events_to_send, (size_t)0);
            // create the BulkRefs for the metadata sizes and contents
            auto metadata_bulk = m_engine.expose(
                    {{metadata_sizes_ptr, num_events_to_send*sizeof(size_t)},
                     {metadata_ptr, metadata_size}},
                    thallium::bulk_mode::read_only);
            auto metadata_size_bulk_ref = BulkRef{
                metadata_bulk, 0, num_events_to_send*sizeof(size_t), self_addr
            };
            auto metadata_bulk_ref = BulkRef{
                metadata_bulk, num_events_to_send*sizeof(size_t), metadata_size, self_addr
            };

            // find the range of descriptor sizes
            const auto descriptors_sizes_ptr = m_events_data_desc_sizes.data() + first_id;
            // find the descriptors content
            const auto descriptors_ptr = m_events_data_desc.data() + m_events_data_desc_offsets[first_id];
            const auto descriptors_size = std::accumulate(
                    descriptors_sizes_ptr, descriptors_sizes_ptr + num_events_to_send, (size_t)0);
            // create BulRefs for the descriptor sizes and contents
            auto data_descriptors_bulk = m_engine.expose(
                    {{descriptors_sizes_ptr, num_events_to_send*sizeof(size_t)},
                     {descriptors_ptr, descriptors_size}},
                    thallium::bulk_mode::read_only);
            // create the BulkRefs for the data descriptors
            auto data_desc_size_bulk_ref = BulkRef{
                data_descriptors_bulk, 0, num_events_to_send*sizeof(size_t), self_addr
            };
            auto data_desc_bulk_ref = BulkRef{
                data_descriptors_bulk, num_events_to_send*sizeof(size_t), descriptors_size, self_addr
            };
            // feed consumer
            consumerHandle.feed(
                    num_events_to_send,
                    first_id,
                    metadata_size_bulk_ref,
                    metadata_bulk_ref,
                    data_desc_size_bulk_ref,
                    data_desc_bulk_ref);

            first_id += num_events_to_send;
        }
    }

    return result;
}

Result<void> MemoryPartitionManager::acknowledge(
    std::string_view consumer_name,
    diaspora::EventID event_id) {
    Result<void> result;
    auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
    std::string consumer_name_str{consumer_name.data(), consumer_name.size()};
    m_consumer_cursor[consumer_name_str] = event_id + 1;
    return result;
}

Result<std::vector<Result<void>>> MemoryPartitionManager::getData(
        const std::vector<diaspora::DataDescriptor>& descriptors,
        const BulkRef& bulk) {
    Result<std::vector<Result<void>>> result;
    result.value().resize(descriptors.size());

    auto client_address = m_engine.lookup(bulk.address);

    std::vector<std::pair<void*, size_t>> local_segments;
    std::unique_lock<thallium::mutex> lock{m_events_data_mtx};
    for(auto& desc : descriptors) {
        OffsetSize event_location;
        event_location.fromDataDescriptor(desc);
        auto flat = desc.flatten();
        for(auto& seg : flat) {
            local_segments.push_back({
                m_events_data.data() + event_location.offset + seg.offset,
                seg.size
            });
        }
    }
    auto local_data_bulk = m_engine.expose(local_segments, thallium::bulk_mode::read_only);
    bulk.handle.on(client_address) << local_data_bulk;

    // TODO there are a few things that would need to be checked in the above code,
    // such as whether we are reading outside of an event's boundary, or whether
    // the remote bulk handle's size matches what is expected.

    return result;
}

Result<bool> MemoryPartitionManager::destroy() {
    Result<bool> result;
    // TODO wait for all the consumers to be done consuming
    result.value() = true;
    return result;
}

std::unique_ptr<mofka::PartitionManager> MemoryPartitionManager::create(
        const thallium::engine& engine,
        const std::string& topic_name,
        const UUID& partition_uuid,
        const diaspora::Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies) {
    (void)dependencies;
    (void)topic_name;
    (void)partition_uuid;
    return std::unique_ptr<mofka::PartitionManager>(
        new MemoryPartitionManager(config, engine));
}

}
