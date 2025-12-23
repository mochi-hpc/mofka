/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROVIDER_IMPL_H
#define MOFKA_PROVIDER_IMPL_H

#include "PartitionManager.hpp"
#include "Provider.hpp"
#include "CerealArchiveAdaptor.hpp"
#include "ConsumerHandleImpl.hpp"

#include <diaspora/DataDescriptor.hpp>
#include <diaspora/Metadata.hpp>

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

    tl::engine         m_engine;
    diaspora::Metadata m_config;
    UUID               m_uuid;
    std::string        m_topic;
    tl::pool           m_pool;
    // RPCs for PartitionManagers
    tl::auto_remote_procedure m_producer_send_batch;
    tl::auto_remote_procedure m_consumer_request_events;
    tl::auto_remote_procedure m_consumer_ack_event;
    tl::auto_remote_procedure m_consumer_remove_consumer;
    tl::auto_remote_procedure m_consumer_request_data;
    /* RPC for Consumers */
    thallium::remote_procedure m_consumer_recv_batch;
    // PartitionManager
    std::shared_ptr<PartitionManager> m_partition_manager;
    // Active consumers
    std::unordered_map<ConsumerKey,
                       std::shared_ptr<ConsumerHandleImpl>,
                       ConsumerKey::Hash>      m_consumers;
    tl::mutex                                  m_consumers_mtx;
    tl::condition_variable                     m_consumers_cv;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id,
                 const diaspora::Metadata& config, const tl::pool& pool,
                 const bedrock::ResolvedDependencyMap& dependencies)
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
        /* Validate the configuration */
        ValidateConfig(config);

        /* Copy the configuration */
        m_config = config;
        m_uuid = UUID::from_string(m_config.json()["uuid"].get_ref<const std::string&>().c_str());
        m_topic = m_config.json()["topic"].get<std::string>();

        std::string partition_type = m_config.json()["type"].get<std::string>();
        auto partition_config = m_config.json().contains("partition")
            ? diaspora::Metadata{m_config.json()["partition"]} : diaspora::Metadata{};

        /* Create the partition manager */
        m_partition_manager = PartitionManagerFactory::create(
            partition_type, get_engine(), m_topic, m_uuid,
            partition_config, dependencies);

        spdlog::trace("[mofka:{0}] Registered provider {1} with uuid {0}", id(), m_uuid.to_string());
    }

    static void ValidateConfig(const diaspora::Metadata& config) {
        /* Schema for any provider configuration */
        static const nlohmann::json configSchema = R"(
        {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "type": "object",
            "properties": {
                "uuid": { "type": "string" },
                "type": { "type": "string" },
                "topic": { "type": "string" },
                "partition": { "type": "object" }
            },
            "required": ["uuid", "type", "topic"]
        }
        )"_json;
        static JsonSchemaValidator jsonValidator{configSchema};

        /* Validate configuration against schema */
        auto errors = jsonValidator.validate(config.json());
        if(!errors.empty()) {
            spdlog::error("[mofka] Error(s) while validating JSON config for provider:");
            for(auto& error : errors) spdlog::error("[mofka] \t{}", error);
            throw diaspora::Exception{"Error(s) while validating JSON config for provider"};
        }
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
        spdlog::trace("[mofka:{}] Received receiveBatch request (topic: {}, count:{})", id(), m_topic, count);
        Result<diaspora::EventID> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        try {
            ENSURE_VALID_PARTITION_MANAGER(result);
            result = m_partition_manager->receiveBatch(
                req.get_endpoint(), producer_name, count, metadata, data);
        } catch(const diaspora::Exception& ex) {
            result.error() = ex.what();
            result.success() = false;
        }
        spdlog::trace("[mofka:{}] Done executing receiveBatch", id());
    }

    void requestEvents(const tl::request& req,
                       intptr_t consumer_ctx,
                       size_t partition_index,
                       const std::string& consumer_name,
                       size_t count,
                       size_t batch_size) {
        spdlog::trace("[mofka:{}] Received requestEvents request"
                      " (topic: {}, partition: {}, count: {}, batchsize: {})",
                      id(), m_topic, partition_index, count, batch_size);
        std::shared_ptr<ConsumerHandleImpl> consumer_handle_impl;
        auto consumer_key = ConsumerKey{consumer_ctx, req.get_endpoint(), partition_index};

        {
            Result<void> result;
            tl::auto_respond<decltype(result)> ensureResponse(req, result);
            try {
                ENSURE_VALID_PARTITION_MANAGER(result);
                consumer_handle_impl = std::make_shared<ConsumerHandleImpl>(
                        consumer_ctx, partition_index,
                        consumer_name, count, m_partition_manager,
                        req.get_endpoint(),
                        m_consumer_recv_batch);
                {
                    auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
                    m_consumers.emplace(consumer_key, consumer_handle_impl);
                }
                m_consumers_cv.notify_all();
            } catch(const diaspora::Exception& ex) {
                result.error() = ex.what();
                result.success() = false;
            }
        } // response is sent here

        m_partition_manager->feedConsumer(consumer_handle_impl, diaspora::BatchSize{batch_size});
        {
            auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
            m_consumers.erase(consumer_key);
        }
        spdlog::trace("[mofka:{}] Done executing requestEvents", id());
    }

    void acknowledge(const tl::request& req,
                     const std::string& consumer_name,
                     diaspora::EventID eventID) {
        spdlog::trace("[mofka:{}] Received acknoweldge request (topic: {}, consumer: {}, event_id: {})",
                      id(), m_topic, consumer_name, eventID);
        Result<void> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        try {
            ENSURE_VALID_PARTITION_MANAGER(result);
            result = m_partition_manager->acknowledge(consumer_name, eventID);
        } catch(const diaspora::Exception& ex) {
            result.error() = ex.what();
            result.success() = false;
        }
        spdlog::trace("[mofka:{}] Done executing acknowledge", id());
    }

    void removeConsumer(const tl::request& req,
                        intptr_t consumer_ctx,
                        size_t partition_index) {
        spdlog::trace("[mofka:{}] Received removeConsumer request (topic: {}, partition: {})",
                      id(), m_topic, partition_index);
        Result<void> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        try {
            auto consumer_key = ConsumerKey{consumer_ctx, req.get_endpoint(), partition_index};
            std::shared_ptr<ConsumerHandleImpl> consumer_handle_impl;
            {
                auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
                auto it = m_consumers.find(consumer_key);
                if(it != m_consumers.end()) {
                    consumer_handle_impl = it->second;
                    m_consumers.erase(it);
                }
            }
            if(consumer_handle_impl) consumer_handle_impl->stop();
        } catch(const diaspora::Exception& ex) {
            result.error() = ex.what();
            result.success() = false;
        }
        spdlog::trace("[mofka:{}] Done executing removeConsumer", id());
    }

    void requestData(const tl::request& req,
                     const Cerealized<diaspora::DataDescriptor>& descriptor,
                     const BulkRef& remote_bulk) {
        spdlog::trace("[mofka:{}] Received requestData request (topic: {})", id(), m_topic);
        Result<std::vector<Result<void>>> result;
        tl::auto_respond<decltype(result)> ensureResponse(req, result);
        try {
            ENSURE_VALID_PARTITION_MANAGER(result);
            result = m_partition_manager->getData({descriptor.content}, remote_bulk);
        } catch(const diaspora::Exception& ex) {
            result.error() = ex.what();
            result.success() = false;
        }
        spdlog::trace("[mofka:{}] Done executing requestData", id());
    }
};

}

#endif
