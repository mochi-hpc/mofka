/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROVIDER_IMPL_H
#define MOFKA_PROVIDER_IMPL_H

#include "mofka/TopicManager.hpp"
#include "mofka/UUID.hpp"
#include "MetadataImpl.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <spdlog/spdlog.h>

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

#define FIND_TOPIC_BY_UUID(__var__, __uuid__) \
        std::shared_ptr<TopicManager> __var__;\
        do {\
            std::lock_guard<tl::mutex> lock(m_topics_mtx);\
            auto it = m_topics_by_uuid.find(__uuid__);\
            if(it == m_topics_by_uuid.end()) {\
                result.success() = false;\
                result.error() = fmt::format("Topic with id \"{}\" not found", __uuid__.to_string());\
                req.respond(result);\
                spdlog::error("[mofka:{}] Topic with id \"{}\" not found", id(), __uuid__.to_string());\
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
    tl::pool             m_pool;
    AutoDeregisteringRPC m_create_topic;
    AutoDeregisteringRPC m_open_topic;
    // RPCs for TopicManagers
    AutoDeregisteringRPC m_compute_sum;
    // TopicManagers
    std::unordered_map<UUID, std::shared_ptr<TopicManager>>        m_topics_by_uuid;
    std::unordered_map<std::string, std::shared_ptr<TopicManager>> m_topics_by_name;
    tl::mutex m_topics_mtx;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_create_topic(define("mofka_create_topic", &ProviderImpl::createTopic, pool))
    , m_open_topic(define("mofka_open_topic", &ProviderImpl::openTopic, pool))
    , m_compute_sum(define("mofka_compute_sum",  &ProviderImpl::computeSum, pool))
    {
        spdlog::trace("[mofka:{0}] Registered provider with id {0}", id());
    }

    void createTopic(const tl::request& req,
                     const std::string& topic_name,
                     Metadata backend_config,
                     Metadata validator_meta,
                     Metadata serializer_meta) {

        spdlog::trace("[mofka:{}] Received createTopic request", id());
        spdlog::trace("[mofka:{}] => name   = {}", id(), topic_name);

        auto topic_id = UUID::generate();
        RequestResult<UUID> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);

        std::string topic_type = "mofka";
        auto& backend_config_json = backend_config.json();
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
            m_topics_by_uuid[topic_id]   = topic;
            m_topics_by_name[topic_name] = topic;
            result.value() = topic_id;
        }

        spdlog::trace("[mofka:{}] Successfully created topic \"{}\" with id {} of type {}",
                id(), topic_name, topic_id.to_string(), topic_type);
    }

    void openTopic(const tl::request& req,
                   const std::string& topic_name) {
        spdlog::trace("[mofka:{}] Received openTopic request for topic {}", id(), topic_name);
        RequestResult<bool> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);
        FIND_TOPIC_BY_NAME(topic, topic_name);
        result.success() = true;
        spdlog::trace("[mofka:{}] Code successfully executed on topic {}", id(), topic_name);
    }

    void computeSum(const tl::request& req,
                    const UUID& topic_id,
                    int32_t x, int32_t y) {
        spdlog::trace("[mofka:{}] Received sayHello request for topic {}", id(), topic_id.to_string());
        RequestResult<int32_t> result;
        AutoResponse<decltype(result)> ensureResponse(req, result);
        FIND_TOPIC_BY_UUID(topic, topic_id);
        result = topic->computeSum(x, y);
        spdlog::trace("[mofka:{}] Successfully executed computeSum on topic {}", id(), topic_id.to_string());
    }

};

}

#endif
