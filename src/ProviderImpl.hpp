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

#define FIND_TOPIC(__var__) \
        std::shared_ptr<TopicManager> __var__;\
        do {\
            std::lock_guard<tl::mutex> lock(m_backends_mtx);\
            auto it = m_backends.find(topic_id);\
            if(it == m_backends.end()) {\
                result.success() = false;\
                result.error() = "Topic with UUID "s + topic_id.to_string() + " not found";\
                req.respond(result);\
                spdlog::error("[mofka:{}] Topic {} not found", id(), topic_id.to_string());\
                return;\
            }\
            __var__ = it->second;\
        }while(0)

namespace mofka {

using namespace std::string_literals;
namespace tl = thallium;

class ProviderImpl : public tl::provider<ProviderImpl> {

    auto id() const { return get_provider_id(); } // for convenience

    public:

    tl::engine           m_engine;
    tl::pool             m_pool;
    // Admin RPC
    tl::remote_procedure m_create_topic;
    tl::remote_procedure m_destroy_topic;
    // Client RPC
    tl::remote_procedure m_check_topic;
    tl::remote_procedure m_say_hello;
    tl::remote_procedure m_compute_sum;
    // TopicManagers
    std::unordered_map<UUID, std::shared_ptr<TopicManager>> m_backends;
    tl::mutex m_backends_mtx;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_engine(engine)
    , m_pool(pool)
    , m_create_topic(define("mofka_create_topic", &ProviderImpl::createTopic, pool))
    , m_destroy_topic(define("mofka_destroy_topic", &ProviderImpl::destroyTopic, pool))
    , m_check_topic(define("mofka_check_topic", &ProviderImpl::checkTopic, pool))
    , m_say_hello(define("mofka_say_hello", &ProviderImpl::sayHello, pool))
    , m_compute_sum(define("mofka_compute_sum",  &ProviderImpl::computeSum, pool))
    {
        spdlog::trace("[mofka:{0}] Registered provider with id {0}", id());
    }

    ~ProviderImpl() {
        spdlog::trace("[mofka:{}] Deregistering provider", id());
        m_create_topic.deregister();
        m_destroy_topic.deregister();
        m_check_topic.deregister();
        m_say_hello.deregister();
        m_compute_sum.deregister();
        spdlog::trace("[mofka:{}]    => done!", id());
    }

    void createTopic(const tl::request& req,
                     const std::string& topic_type,
                     const std::string& topic_config) {

        spdlog::trace("[mofka:{}] Received createTopic request", id());
        spdlog::trace("[mofka:{}]    => type = {}", id(), topic_type);
        spdlog::trace("[mofka:{}]    => config = {}", id(), topic_config);

        auto topic_id = UUID::generate();
        RequestResult<UUID> result;

        rapidjson::Document json_config;
        rapidjson::ParseResult ok = json_config.Parse(topic_config.c_str());
        if(!ok) {
            result.error() = rapidjson::GetParseError_En(ok.Code());
            result.success() = false;
            spdlog::error("[mofka:{}] Could not parse topic configuration for topic {}: {} ({})",
                          id(), topic_id.to_string(), rapidjson::GetParseError_En(ok.Code()),
                          ok.Offset());
            req.respond(result);
            return;
        }

        std::unique_ptr<TopicManager> backend;
        try {
            backend = TopicFactory::createTopic(topic_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            spdlog::error("[mofka:{}] Error when creating topic {} of type {}:",
                    id(), topic_id.to_string(), topic_type);
            spdlog::error("[mofka:{}]    => {}", id(), result.error());
            req.respond(result);
            return;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown topic type "s + topic_type;
            spdlog::error("[mofka:{}] Unknown topic type {} for topic {}",
                    id(), topic_type, topic_id.to_string());
            req.respond(result);
            return;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[topic_id] = std::move(backend);
            result.value() = topic_id;
        }

        req.respond(result);
        spdlog::trace("[mofka:{}] Successfully created topic {} of type {}",
                id(), topic_id.to_string(), topic_type);
    }

    void destroyTopic(const tl::request& req,
                      const UUID& topic_id) {
        RequestResult<bool> result;
        spdlog::trace("[mofka:{}] Received destroyTopic request for topic {}", id(), topic_id.to_string());

        {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);

            if(m_backends.count(topic_id) == 0) {
                result.success() = false;
                result.error() = "Topic "s + topic_id.to_string() + " not found";
                req.respond(result);
                spdlog::error("[mofka:{}] Topic {} not found", id(), topic_id.to_string());
                return;
            }

            result = m_backends[topic_id]->destroy();
            m_backends.erase(topic_id);
        }

        req.respond(result);
        spdlog::trace("[mofka:{}] Topic {} successfully destroyed", id(), topic_id.to_string());
    }

    void checkTopic(const tl::request& req,
                       const UUID& topic_id) {
        spdlog::trace("[mofka:{}] Received checkTopic request for topic {}", id(), topic_id.to_string());
        RequestResult<bool> result;
        FIND_TOPIC(topic);
        result.success() = true;
        req.respond(result);
        spdlog::trace("[mofka:{}] Code successfully executed on topic {}", id(), topic_id.to_string());
    }

    void sayHello(const tl::request& req,
                  const UUID& topic_id) {
        spdlog::trace("[mofka:{}] Received sayHello request for topic {}", id(), topic_id.to_string());
        RequestResult<bool> result;
        FIND_TOPIC(topic);
        topic->sayHello();
        spdlog::trace("[mofka:{}] Successfully executed sayHello on topic {}", id(), topic_id.to_string());
    }

    void computeSum(const tl::request& req,
                    const UUID& topic_id,
                    int32_t x, int32_t y) {
        spdlog::trace("[mofka:{}] Received sayHello request for topic {}", id(), topic_id.to_string());
        RequestResult<int32_t> result;
        FIND_TOPIC(topic);
        result = topic->computeSum(x, y);
        req.respond(result);
        spdlog::trace("[mofka:{}] Successfully executed computeSum on topic {}", id(), topic_id.to_string());
    }

};

}

#endif
