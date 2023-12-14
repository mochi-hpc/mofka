/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROVIDER_IMPL_H
#define MOFKA_PROVIDER_IMPL_H

#include "mofka/PartitionManager.hpp"
#include "mofka/DataDescriptor.hpp"
#include "CerealArchiveAdaptor.hpp"
#include "ConsumerHandleImpl.hpp"
#include "MetadataImpl.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <spdlog/spdlog.h>

#include <unordered_map>
#include <tuple>

namespace mofka {

using namespace std::string_literals;
namespace tl = thallium;

class ProviderImpl : public tl::provider<ProviderImpl> {

    auto id() const { return get_provider_id(); } // for convenience

    public:

    tl::engine           m_engine;
    rapidjson::Document  m_config;
    UUID                 m_uuid;
    tl::pool             m_pool;
    // RPCs for PartitionManagers
    tl::auto_remote_procedure m_producer_send_batch;
    tl::auto_remote_procedure m_consumer_request_events;
    tl::auto_remote_procedure m_consumer_ack_event;
    tl::auto_remote_procedure m_consumer_remove_consumer;
    tl::auto_remote_procedure m_consumer_request_data;
    /* RPC for Consumers */
    thallium::remote_procedure m_consumer_recv_batch;
    // PartitionManager
    SP<PartitionManager> m_partition_manager;
    // Active consumers
    std::unordered_map<UUID, SP<ConsumerHandleImpl>> m_consumers;
    tl::mutex                                        m_consumers_mtx;
    tl::condition_variable                           m_consumers_cv;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id,
                 const rapidjson::Value& config, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_producer_send_batch(define("mofka_producer_send_batch",  &ProviderImpl::receiveBatch, pool))
    , m_consumer_request_events(define("mofka_consumer_request_events", &ProviderImpl::requestEvents, pool))
    , m_consumer_ack_event(define("mofka_consumer_ack_event", &ProviderImpl::acknowledge, pool))
    , m_consumer_remove_consumer(define("mofka_consumer_remove_consumer", &ProviderImpl::removeConsumer, pool))
    , m_consumer_request_data(define("mofka_consumer_request_data", &ProviderImpl::requestData, pool))
    , m_consumer_recv_batch(m_engine.define("mofka_consumer_recv_batch"))
    {
        m_config.CopyFrom(config, m_config.GetAllocator(), true);
        if(m_config.HasMember("uuid") && m_config["uuid"].IsString()) {
            m_uuid = UUID::from_string(m_config["uuid"].GetString());
        } else {
            m_uuid = UUID::generate();
        }
        auto uuid = m_uuid.to_string();
        rapidjson::Value uuid_value;
        uuid_value.SetString(uuid.c_str(), uuid.size(), m_config.GetAllocator());
        m_config.AddMember("uuid", std::move(uuid_value), m_config.GetAllocator());

        // FIXME
        // For now we just instanciate a MemoryPartitionManager
        m_partition_manager = PartitionManagerFactory::create(
            "memory", get_engine(),
            Metadata{"{}"},  // config
            Metadata{"{}"},  // validator
            Metadata{"{}"},  // selector
            Metadata{"{}"}); // serializer

        spdlog::trace("[mofka:{0}] Registered provider {1} with uuid {0}", id(), uuid);
    }

    #define ENSURE_VALID_PARTITION_MANAGER(__result__) do { \
        if(!m_partition_manager) { \
            __result__.error() = "No partition manager attached to this provider"; \
            __result__.success() = false; \
            return; \
        } \
    } while(0)


    void receiveBatch(const tl::request& req,
                      const std::string& producer_name,
                      size_t count,
                      const BulkRef& metadata,
                      const BulkRef& data) {
        spdlog::trace("[mofka:{}] Received receiveBatch request", id());
        Result<EventID> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        ENSURE_VALID_PARTITION_MANAGER(result);
        result = m_partition_manager->receiveBatch(
            req.get_endpoint(), producer_name, count, metadata, data);
        spdlog::trace("[mofka:{}] Successfully executed receiveBatch", id());
    }

    void requestEvents(const tl::request& req,
                       intptr_t consumer_ctx,
                       size_t target_info_index,
                       const UUID& consumer_id,
                       const std::string& consumer_name,
                       size_t count,
                       size_t batch_size) {
        spdlog::trace("[mofka:{}] Received requestEvents request", id());
        Result<void> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        ENSURE_VALID_PARTITION_MANAGER(result);
        auto consumer_handle_impl = std::make_shared<ConsumerHandleImpl>(
            consumer_ctx, target_info_index,
            consumer_name, count, m_partition_manager,
            req.get_endpoint(),
            m_consumer_recv_batch);
        {
            auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
            m_consumers.emplace(consumer_id, consumer_handle_impl);
        }
        m_consumers_cv.notify_all();
        result = m_partition_manager->feedConsumer(consumer_handle_impl, BatchSize{batch_size});
        {
            auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
            m_consumers.erase(consumer_id);
        }
        spdlog::trace("[mofka:{}] Successfully executed requestEvents", id());
    }

    void acknowledge(const tl::request& req,
                     const std::string& consumer_name,
                     EventID eventID) {
        spdlog::trace("[mofka:{}] Received acknoweldge request", id());
        Result<void> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        ENSURE_VALID_PARTITION_MANAGER(result);
        result = m_partition_manager->acknowledge(consumer_name, eventID);
        spdlog::trace("[mofka:{}] Successfully executed acknowledge", id());
    }

    void removeConsumer(const tl::request& req,
                        const UUID& consumer_id) {
        spdlog::trace("[mofka:{}] Received removeConsumer request", id());
        Result<void> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        SP<ConsumerHandleImpl> consumer_handle_impl;
        while(!consumer_handle_impl) {
            auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
            auto it = m_consumers.find(consumer_id);
            if(it != m_consumers.end()) {
                consumer_handle_impl = it->second;
                m_consumers.erase(it);
            }
            if(!consumer_handle_impl)
                m_consumers_cv.wait(g);
        }
        consumer_handle_impl->stop();
        spdlog::trace("[mofka:{}] Successfully executed removeConsumer", id());
    }

    void requestData(const tl::request& req,
                     const Cerealized<DataDescriptor>& descriptor,
                     const BulkRef& remote_bulk) {
        spdlog::trace("[mofka:{}] Received requestData request", id());
        Result<std::vector<Result<void>>> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        ENSURE_VALID_PARTITION_MANAGER(result);
        result = m_partition_manager->getData({descriptor.content}, remote_bulk);
        spdlog::trace("[mofka:{}] Successfully executed requestData", id());
    }

};

}

#endif
