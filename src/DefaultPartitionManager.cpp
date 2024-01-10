/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "RapidJsonUtil.hpp"
#include "DefaultPartitionManager.hpp"
#include "mofka/DataDescriptor.hpp"
#include "mofka/BufferWrapperArchive.hpp"
#include "RapidJsonUtil.hpp"
#include <rapidjson/writer.h>
#include <spdlog/spdlog.h>
#include <numeric>
#include <iostream>


namespace mofka {

MOFKA_REGISTER_PARTITION_MANAGER_WITH_DEPENDENCIES(
    default, DefaultPartitionManager,
    {"data", "warabi", BEDROCK_REQUIRED},
    {"metadata", "yokan", BEDROCK_REQUIRED});

Result<EventID> DefaultPartitionManager::receiveBatch(
          const thallium::endpoint& sender,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    (void)producer_name;
    Result<EventID> result;
    EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_events_metadata_mtx};
        first_id = m_events_metadata_sizes.size();
        // --------- transfer the metadata
        auto metadata_size = metadata_bulk.size - num_events*sizeof(size_t);
        // resize the sizes and offsets arrays, and metadata array
        if(m_events_metadata_sizes.capacity() < first_id + num_events) {
            m_events_metadata_sizes.reserve(2*(first_id + num_events));
        }
        if(m_events_metadata_offsets.capacity() < first_id + num_events) {
            m_events_metadata_offsets.reserve(2*(first_id + num_events));
        }
        m_events_metadata_sizes.resize(first_id + num_events);
        m_events_metadata_offsets.resize(first_id + num_events);
        size_t first_metadata_offset = m_events_metadata.size();
        if(m_events_metadata.capacity() < first_metadata_offset + metadata_size) {
            m_events_metadata.reserve(2*(first_metadata_offset + metadata_size));
        }
        m_events_metadata.resize(first_metadata_offset + metadata_size);
        // transfer the metadata sizes and content
        auto local_metadata_bulk = m_engine.expose(
            {{(char*)(m_events_metadata_sizes.data() + first_id), num_events*sizeof(size_t)},
             {m_events_metadata.data() + first_metadata_offset, metadata_size}},
            thallium::bulk_mode::write_only);
        local_metadata_bulk << metadata_bulk.handle.on(sender).select(
            metadata_bulk.offset, metadata_bulk.size);
        // TODO check that metadata_size = sum of m_events_metadata_sizes recveived
        // update the metadata offsets vector
        auto metadata_offset = first_metadata_offset;
        for(size_t i = first_id; i < m_events_metadata_sizes.size(); ++i) {
            m_events_metadata_offsets[i] = metadata_offset;
            metadata_offset += m_events_metadata_sizes[i];
        }
        // --------- transfer the data to the DataStore
        auto descriptors = m_data_store->store(num_events, data_bulk);
        if(!descriptors.success()) {
            result.success() = false;
            result.error() = descriptors.error();
            return result;
        }

        // update the list of DataDescriptors
        size_t data_desc_offset = 0;
        if(!m_events_data_desc_offsets.empty())
            data_desc_offset = m_events_data_desc_offsets.back() + m_events_data_desc_sizes.back();
        m_events_data_desc_sizes.resize(first_id + num_events);
        m_events_data_desc_offsets.resize(first_id + num_events);
        BufferWrapperOutputArchive output_archive{m_events_data_desc};
        for(size_t i = first_id; i < m_events_data_desc_sizes.size(); ++i) {
            const auto& data_descriptor = descriptors.value()[i - first_id];
            size_t m_events_data_desc_size = m_events_data_desc.size();
            data_descriptor.save(output_archive);
            auto data_descriptor_size = m_events_data_desc.size() - m_events_data_desc_size;
            m_events_data_desc_sizes[i] = data_descriptor_size;
            m_events_data_desc_offsets[i] = data_desc_offset;
            data_desc_offset += data_descriptor_size;
        }
    }
    m_events_cv.notify_all();
    result.value() = first_id;
    return result;
}

void DefaultPartitionManager::wakeUp() {
    m_events_cv.notify_all();
}

Result<void> DefaultPartitionManager::feedConsumer(
    ConsumerHandle consumerHandle,
    BatchSize batchSize) {
    Result<void> result;

    if(batchSize.value == 0)
        batchSize = BatchSize::Adaptive();
    EventID first_id;
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

Result<void> DefaultPartitionManager::acknowledge(
    std::string_view consumer_name,
    EventID event_id) {
    Result<void> result;
    auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
    std::string consumer_name_str{consumer_name.data(), consumer_name.size()};
    m_consumer_cursor[consumer_name_str] = event_id + 1;
    return result;
}

Result<std::vector<Result<void>>> DefaultPartitionManager::getData(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& bulk) {
    return m_data_store->load(descriptors, bulk);
}

Result<bool> DefaultPartitionManager::destroy() {
    Result<bool> result;
    // TODO wait for all the consumers to be done consuming
    result.value() = true;
    return result;
}

std::unique_ptr<mofka::PartitionManager> DefaultPartitionManager::create(
        const thallium::engine& engine,
        const std::string& topic_name,
        const UUID& partition_uuid,
        const Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies) {

    static constexpr const char* configSchema = R"(
    {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "type": "object",
        "properties":{}
    }
    )";

    /* Validate configuration against schema */
    static RapidJsonValidator schemaValidator{configSchema};
    auto validationErrors = schemaValidator.validate(config.json());
    if(!validationErrors.empty()) {
        spdlog::error("[mofka] Error(s) while validating JSON config for DefaultPartitionManager:");
        for(auto& error : validationErrors) spdlog::error("[mofka] \t{}", error);
        throw Exception{"Error(s) while validating JSON config for DefaultPartitionManager"};
    }

    /* Check that the dependencies are there */
    auto it = dependencies.find("data");
    warabi::TargetHandle* warabi_target = nullptr;;
    if(it == dependencies.end()) {
        throw Exception{"Warabi TargetHandle not provided as dependency"};
    } else {
        warabi_target = it->second.dependencies[0]->getHandle<warabi::TargetHandle*>();
    }

    yk_database_handle_t yokan_db = nullptr;
    it = dependencies.find("metadata");
    if(it == dependencies.end()) {
        throw Exception{"Yokan Database not provided as dependency"};
    } else {
        yokan_db = it->second.dependencies[0]->getHandle<yk_database_handle_t>();
    }

    /* create data store */
    auto data_store = WarabiDataStore::create(engine, std::move(*warabi_target));

    /* create event store */
    auto event_store = YokanEventStore::create(engine, topic_name, partition_uuid, yokan_db);

    /* create topic manager */
    return std::unique_ptr<mofka::PartitionManager>(
        new DefaultPartitionManager(std::move(config),
                                    std::move(data_store),
                                    std::move(event_store),
                                    engine));
}

}
