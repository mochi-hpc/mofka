/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "DefaultPartitionManager.hpp"
#include "mofka/DataDescriptor.hpp"
#include "mofka/BufferWrapperArchive.hpp"
#include <spdlog/spdlog.h>
#include <numeric>
#include <iostream>

namespace tl = thallium;

namespace mofka {

MOFKA_REGISTER_PARTITION_MANAGER_WITH_DEPENDENCIES(
    default, DefaultPartitionManager,
    {"data", "warabi", true, false, false},
    {"metadata", "yokan", true, false, false});

Result<EventID> DefaultPartitionManager::receiveBatch(
          const thallium::endpoint& sender,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    (void)producer_name;
    (void)sender;
    Result<EventID> first_id;

    // --------- asynchronously transfer the data to the DataStore
    auto future_descriptors = m_data_store->store(num_events, data_bulk);

    // --------- meanwhile transfer the metadata to the EventStore
    first_id = m_event_store->appendMetadata(num_events, metadata_bulk);
    if(!first_id.success()) return first_id;

    // --------- wait for the data transfers
    std::vector<DataDescriptor> descriptors;
    try {
        descriptors = future_descriptors.wait();
    } catch(const std::exception& ex) {
        first_id.success() = false;
        first_id.error() = ex.what();
        return first_id;
    }

    // --------- transfer the descriptors
    auto ok = m_event_store->storeDataDescriptors(first_id.value(), descriptors);
    if(!ok.success()) {
        first_id.success() = false;
        first_id.error() = ok.error();
        return first_id;
    }

    wakeUp();

    return first_id;
}

void DefaultPartitionManager::wakeUp() {
    m_event_store->wakeUp();
}

Result<void> DefaultPartitionManager::feedConsumer(
    ConsumerHandle consumerHandle,
    BatchSize batchSize) {
    Result<void> result;

    EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
        if(m_consumer_cursor.count(consumerHandle.name()) == 0) {
            m_consumer_cursor[consumerHandle.name()] = 0;
        }
        first_id = m_consumer_cursor[consumerHandle.name()];
    }
    m_event_store->feed(consumerHandle, first_id, batchSize);

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

Result<void> DefaultPartitionManager::markAsComplete() {
    return m_event_store->markAsComplete();
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

    static const nlohmann::json configSchema = R"(
    {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "type": "object",
        "properties":{}
    }
    )"_json;

    /* Validate configuration against schema */
    static JsonSchemaValidator schemaValidator{configSchema};
    auto validationErrors = schemaValidator.validate(config.json());
    if(!validationErrors.empty()) {
        spdlog::error("[mofka] Error(s) while validating JSON config for DefaultPartitionManager:");
        for(auto& error : validationErrors) spdlog::error("[mofka] \t{}", error);
        throw Exception{"Error(s) while validating JSON config for DefaultPartitionManager"};
    }

    /* the data and metadata dependencies are required so we know they are in the map */
    auto warabi_ph = dependencies.at("data")[0]->getHandle<tl::provider_handle>();
    auto yokan_ph =  dependencies.at("metadata")[0]->getHandle<tl::provider_handle>();

    /* pool is an optional dependency */
    tl::pool pool;
    if(dependencies.count("pool")) {
        pool = dependencies.at("pool")[0]->getHandle<tl::pool>();
    } else {
        pool = engine.get_handler_pool();
    }

    /* create data store */
    auto data_store = WarabiDataStore::create(engine, std::move(warabi_ph), pool);

    /* create event store */
    auto event_store = YokanEventStore::create(engine, topic_name, partition_uuid, std::move(yokan_ph));

    /* create topic manager */
    return std::unique_ptr<mofka::PartitionManager>(
        new DefaultPartitionManager(std::move(config),
                                    std::move(data_store),
                                    std::move(event_store),
                                    engine));
}

}
