/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROVIDER_IMPL_H
#define MOFKA_PROVIDER_IMPL_H

#include "mofka/TopicManager.hpp"
#include "ConsumerHandleImpl.hpp"
#include "mofka/UUID.hpp"
#include "MetadataImpl.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <spdlog/spdlog.h>

#include <unordered_map>
#include <tuple>

#define FIND_TOPIC_BY_NAME(__var__, __name__) \
        std::shared_ptr<TopicManager> __var__;\
        do {\
            std::lock_guard<tl::mutex> lock(m_topics_mtx);\
            auto it = m_topics_by_name.find(__name__);\
            if(it == m_topics_by_name.end()) {\
                result.success() = false;\
                result.error() = fmt::format("Topic with name \"{}\" not found", __name__);\
                req.respond(result);\
                spdlog::error("[mofka:{}] Topic \"{}\" not found", id(), __name__);\
                return;\
            }\
            __var__ = it->second;\
        } while(0)

namespace mofka {

using namespace std::string_literals;
namespace tl = thallium;

struct AutoDeregisteringRPC : public tl::remote_procedure {

    AutoDeregisteringRPC(tl::remote_procedure rpc)
    : tl::remote_procedure(std::move(rpc)) {}

    ~AutoDeregisteringRPC() {
        deregister();
    }
};

template<typename ResponseType>
struct AutoResponse {

    AutoResponse(const tl::request& req, ResponseType& resp)
    : m_req(req)
    , m_resp(resp) {}

    AutoResponse(const AutoResponse&) = delete;
    AutoResponse(AutoResponse&&) = delete;

    ~AutoResponse() {
        m_req.respond(m_resp);
    }

    const tl::request& m_req;
    ResponseType&      m_resp;
};

class ProviderImpl : public tl::provider<ProviderImpl> {

    auto id() const { return get_provider_id(); } // for convenience

    public:

    tl::engine           m_engine;
    rapidjson::Document  m_config;
    UUID                 m_uuid;
    tl::pool             m_pool;
    AutoDeregisteringRPC m_create_topic;
    AutoDeregisteringRPC m_open_topic;
    // RPCs for TopicManagers
    AutoDeregisteringRPC m_producer_send_batch;
    AutoDeregisteringRPC m_consumer_request_events;
    AutoDeregisteringRPC m_consumer_ack_event;
    AutoDeregisteringRPC m_consumer_remove_consumer;
    /* RPC for Consumers */
    thallium::remote_procedure m_consumer_recv_batch;
    // TopicManagers
    std::unordered_map<std::string, std::shared_ptr<TopicManager>> m_topics_by_name;
    tl::mutex m_topics_mtx;
    // Active consumers
    std::unordered_map<UUID, std::shared_ptr<ConsumerHandleImpl>>  m_consumers;
    tl::mutex                                                      m_consumers_mtx;
    tl::condition_variable                                         m_consumers_cv;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id,
                 const rapidjson::Value& config, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_create_topic(define("mofka_create_topic", &ProviderImpl::createTopic, pool))
    , m_open_topic(define("mofka_open_topic", &ProviderImpl::openTopic, pool))
    , m_producer_send_batch(define("mofka_producer_send_batch",  &ProviderImpl::receiveBatch, pool))
    , m_consumer_request_events(define("mofka_consumer_request_events", &ProviderImpl::requestEvents, pool))
    , m_consumer_ack_event(define("mofka_consumer_ack_event", &ProviderImpl::acknowledge, pool))
    , m_consumer_remove_consumer(define("mofka_consumer_remove_consumer", &ProviderImpl::removeConsumer, pool))
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
        spdlog::trace("[mofka:{0}] Registered provider {1} with uuid {0}", id(), uuid);
    }

    void createTopic(const tl::request& req,
                     const std::string& topic_name,
                     Metadata backend_config,
                     Metadata validator_meta,
                     Metadata selector_meta,
                     Metadata serializer_meta) {

        spdlog::trace("[mofka:{}] Received createTopic request", id());
        spdlog::trace("[mofka:{}] => name       = {}", id(), topic_name);

        using ResultType = std::tuple<Metadata, Metadata, Metadata>;
        RequestResult<ResultType> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);

        std::string topic_type = "mofka";
        const auto& backend_config_json = backend_config.json();
        if(backend_config_json.GetType() == rapidjson::kObjectType) {
            auto topic_type_it = backend_config_json.FindMember("__type__");
            if(topic_type_it != backend_config_json.MemberEnd()) {
                if(topic_type_it->value.GetType() != rapidjson::kStringType) {
                    result.success() = false;
                    result.error() = "Invalid type for __type__ field in topic config, expected string";
                    spdlog::error("[mofka:{}] {}", id(), result.error());
                    return;
                }
                topic_type = topic_type_it->value.GetString();
            }
        }

        std::lock_guard<tl::mutex> lock(m_topics_mtx);

        if (m_topics_by_name.find(topic_name) != m_topics_by_name.end()) {
            result.error() = fmt::format("Topic with name \"{}\" already exists", topic_name);
            result.success() = false;
            spdlog::error("[mofka:{}] {}", id(), result.error());
            return;
        }

        std::shared_ptr<TopicManager> topic;
        try {
            topic = TopicFactory::createTopic(
                topic_type, get_engine(),
                backend_config,
                validator_meta,
                selector_meta,
                serializer_meta);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = fmt::format("Error when creating topic \"{}\": {}", ex.what());
            spdlog::error("[mofka:{}] {}", id(), result.error());
            return;
        }

        if(not topic) {
            result.success() = false;
            result.error() = fmt::format(
                "Unknown topic type \"{}\" for topic \"{}\"", topic_type, topic_name);
            spdlog::error("[mofka:{}] {}", id(), result.error());
            return;
        } else {
            m_topics_by_name[topic_name] = topic;
            result.value() = std::make_tuple(
                topic->getValidatorMetadata(),
                topic->getTargetSelectorMetadata(),
                topic->getSerializerMetadata());
        }

        spdlog::trace("[mofka:{}] Successfully created topic \"{}\" of type {}",
                id(), topic_name, topic_type);
    }

    void openTopic(const tl::request& req,
                   const std::string& topic_name) {
        spdlog::trace("[mofka:{}] Received openTopic request for topic {}", id(), topic_name);
        using ResultType = std::tuple<Metadata, Metadata, Metadata>;
        RequestResult<ResultType> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);

        FIND_TOPIC_BY_NAME(topic, topic_name);
        result.success() = true;
        result.value() = std::make_tuple(
                topic->getValidatorMetadata(),
                topic->getTargetSelectorMetadata(),
                topic->getSerializerMetadata());
        spdlog::trace("[mofka:{}] Code successfully executed on topic {}", id(), topic_name);
    }

    void receiveBatch(const tl::request& req,
                      const std::string& topic_name,
                      const std::string& producer_name,
                      size_t count,
                      const BulkRef& metadata,
                      const BulkRef& data) {
        spdlog::trace("[mofka:{}] Received receiveBatch request for topic {}", id(), topic_name);
        RequestResult<EventID> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);
        FIND_TOPIC_BY_NAME(topic, topic_name);
        result = topic->receiveBatch(
            req.get_endpoint(), producer_name, count, metadata, data);
        spdlog::trace("[mofka:{}] Successfully executed receiveBatch on topic {}", id(), topic_name);
    }

    void requestEvents(const tl::request& req,
                       const std::string& topic_name,
                       intptr_t consumer_ctx,
                       size_t target_info_index,
                       const UUID& consumer_id,
                       const std::string& consumer_name,
                       size_t count,
                       size_t batch_size) {
        spdlog::trace("[mofka:{}] Received requestEvents request for topic {}", id(), topic_name);
        RequestResult<void> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);
        FIND_TOPIC_BY_NAME(topic, topic_name);
        auto consumer_handle_impl = std::make_shared<ConsumerHandleImpl>(
            consumer_ctx, target_info_index,
            consumer_name, count, topic,
            req.get_endpoint(),
            m_consumer_recv_batch);
        {
            auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
            m_consumers.emplace(consumer_id, consumer_handle_impl);
        }
        m_consumers_cv.notify_all();
        result = topic->feedConsumer(consumer_handle_impl, BatchSize{batch_size});
        {
            auto g = std::unique_lock<tl::mutex>{m_consumers_mtx};
            m_consumers.erase(consumer_id);
        }
        spdlog::trace("[mofka:{}] Successfully executed requestEvents on topic {}", id(), topic_name);
    }

    void acknowledge(const tl::request& req,
                     const std::string& topic_name,
                     const std::string& consumer_name,
                     EventID eventID) {
        spdlog::trace("[mofka:{}] Received acknoweldge request for topic {}", id(), topic_name);
        RequestResult<void> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);
        FIND_TOPIC_BY_NAME(topic, topic_name);
        result = topic->acknowledge(consumer_name, eventID);
        spdlog::trace("[mofka:{}] Successfully executed acknowledge on topic {}", id(), topic_name);
    }

    void removeConsumer(const tl::request& req,
                        const UUID& consumer_id) {
        spdlog::trace("[mofka:{}] Received removeConsumer request", id());
        RequestResult<void> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);
        std::shared_ptr<ConsumerHandleImpl> consumer_handle_impl;
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

};

}

#endif
