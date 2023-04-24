/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROVIDER_IMPL_H
#define MOFKA_PROVIDER_IMPL_H

#include "mofka/TopicManager.hpp"
#include "mofka/UUID.hpp"

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
                     const std::string& topic_config,
                     const std::string& topic_type) {

        spdlog::trace("[mofka:{}] Received createTopic request", id());
        spdlog::trace("[mofka:{}] => name   = {}", id(), topic_name);
        spdlog::trace("[mofka:{}] => type   = {}", id(), topic_type);
        spdlog::trace("[mofka:{}] => config = {}", id(), topic_config);

        auto topic_id = UUID::generate();
        RequestResult<UUID> result;

        rapidjson::Document json_config;
        rapidjson::ParseResult ok = json_config.Parse(topic_config.c_str());
        if(!ok) {
            result.error() = fmt::format(
                "Could not parse topic configuration for topic \"{}\": {} ({})",
                topic_name, rapidjson::GetParseError_En(ok.Code()), ok.Offset());
            result.success() = false;
            spdlog::error("[mofka:{}] {}", id(), result.error());
            req.respond(result);
            return;
        }

        std::lock_guard<tl::mutex> lock(m_topics_mtx);

        if (m_topics_by_name.find(topic_name) != m_topics_by_name.end()) {
            result.error() = fmt::format("Topic with name \"{}\" already exists", topic_name);
            result.success() = false;
            spdlog::error("[mofka:{}] {}", id(), result.error());
            req.respond(result);
            return;
        }

        std::shared_ptr<TopicManager> topic;
        try {
            topic = TopicFactory::createTopic(topic_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = fmt::format("Error when creating topic \"{}\": {}", ex.what());
            spdlog::error("[mofka:{}] {}", id(), result.error());
            req.respond(result);
            return;
        }

        if(not topic) {
            result.success() = false;
            result.error() = fmt::format(
                "Unknown topic type \"{}\" for topic \"{}\"", topic_type, topic_name);
            spdlog::error("[mofka:{}] {}", id(), result.error());
            req.respond(result);
            return;
        } else {
            m_topics_by_uuid[topic_id]   = topic;
            m_topics_by_name[topic_name] = topic;
            result.value() = topic_id;
        }

        req.respond(result);
        spdlog::trace("[mofka:{}] Successfully created topic \"{}\" with id {} of type {}",
                id(), topic_name, topic_id.to_string(), topic_type);
    }

    void openTopic(const tl::request& req,
                   const std::string& topic_name) {
        spdlog::trace("[mofka:{}] Received openTopic request for topic {}", id(), topic_name);
        RequestResult<bool> result;
        FIND_TOPIC_BY_NAME(topic, topic_name);
        result.success() = true;
        req.respond(result);
        spdlog::trace("[mofka:{}] Code successfully executed on topic {}", id(), topic_name);
    }

    void computeSum(const tl::request& req,
                    const UUID& topic_id,
                    int32_t x, int32_t y) {
        spdlog::trace("[mofka:{}] Received sayHello request for topic {}", id(), topic_id.to_string());
        RequestResult<int32_t> result;
        FIND_TOPIC_BY_UUID(topic, topic_id);
        result = topic->computeSum(x, y);
        req.respond(result);
        spdlog::trace("[mofka:{}] Successfully executed computeSum on topic {}", id(), topic_id.to_string());
    }

};

}

#endif
