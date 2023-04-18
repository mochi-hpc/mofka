/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_PROVIDER_IMPL_H
#define __ALPHA_PROVIDER_IMPL_H

#include "alpha/Backend.hpp"
#include "alpha/UUID.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <tuple>

#define FIND_RESOURCE(__var__) \
        std::shared_ptr<Backend> __var__;\
        do {\
            std::lock_guard<tl::mutex> lock(m_backends_mtx);\
            auto it = m_backends.find(resource_id);\
            if(it == m_backends.end()) {\
                result.success() = false;\
                result.error() = "Resource with UUID "s + resource_id.to_string() + " not found";\
                req.respond(result);\
                spdlog::error("[provider:{}] Resource {} not found", id(), resource_id.to_string());\
                return;\
            }\
            __var__ = it->second;\
        }while(0)

namespace alpha {

using namespace std::string_literals;
namespace tl = thallium;

class ProviderImpl : public tl::provider<ProviderImpl> {

    auto id() const { return get_provider_id(); } // for convenience

    using json = nlohmann::json;

    public:

    std::string          m_token;
    tl::pool             m_pool;
    // Admin RPC
    tl::remote_procedure m_create_resource;
    tl::remote_procedure m_open_resource;
    tl::remote_procedure m_close_resource;
    tl::remote_procedure m_destroy_resource;
    // Client RPC
    tl::remote_procedure m_check_resource;
    tl::remote_procedure m_say_hello;
    tl::remote_procedure m_compute_sum;
    // Backends
    std::unordered_map<UUID, std::shared_ptr<Backend>> m_backends;
    tl::mutex m_backends_mtx;

    ProviderImpl(const tl::engine& engine, uint16_t provider_id, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_pool(pool)
    , m_create_resource(define("alpha_create_resource", &ProviderImpl::createResource, pool))
    , m_open_resource(define("alpha_open_resource", &ProviderImpl::openResource, pool))
    , m_close_resource(define("alpha_close_resource", &ProviderImpl::closeResource, pool))
    , m_destroy_resource(define("alpha_destroy_resource", &ProviderImpl::destroyResource, pool))
    , m_check_resource(define("alpha_check_resource", &ProviderImpl::checkResource, pool))
    , m_say_hello(define("alpha_say_hello", &ProviderImpl::sayHello, pool))
    , m_compute_sum(define("alpha_compute_sum",  &ProviderImpl::computeSum, pool))
    {
        spdlog::trace("[provider:{0}] Registered provider with id {0}", id());
    }

    ~ProviderImpl() {
        spdlog::trace("[provider:{}] Deregistering provider", id());
        m_create_resource.deregister();
        m_open_resource.deregister();
        m_close_resource.deregister();
        m_destroy_resource.deregister();
        m_check_resource.deregister();
        m_say_hello.deregister();
        m_compute_sum.deregister();
        spdlog::trace("[provider:{}]    => done!", id());
    }

    void createResource(const tl::request& req,
                        const std::string& token,
                        const std::string& resource_type,
                        const std::string& resource_config) {

        spdlog::trace("[provider:{}] Received createResource request", id());
        spdlog::trace("[provider:{}]    => type = {}", id(), resource_type);
        spdlog::trace("[provider:{}]    => config = {}", id(), resource_config);

        auto resource_id = UUID::generate();
        RequestResult<UUID> result;

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            req.respond(result);
            spdlog::error("[provider:{}] Invalid security token {}", id(), token);
            return;
        }

        json json_config;
        try {
            json_config = json::parse(resource_config);
        } catch(json::parse_error& e) {
            result.error() = e.what();
            result.success() = false;
            spdlog::error("[provider:{}] Could not parse resource configuration for resource {}",
                    id(), resource_id.to_string());
            req.respond(result);
            return;
        }

        std::unique_ptr<Backend> backend;
        try {
            backend = ResourceFactory::createResource(resource_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            spdlog::error("[provider:{}] Error when creating resource {} of type {}:",
                    id(), resource_id.to_string(), resource_type);
            spdlog::error("[provider:{}]    => {}", id(), result.error());
            req.respond(result);
            return;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown resource type "s + resource_type;
            spdlog::error("[provider:{}] Unknown resource type {} for resource {}",
                    id(), resource_type, resource_id.to_string());
            req.respond(result);
            return;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[resource_id] = std::move(backend);
            result.value() = resource_id;
        }
        
        req.respond(result);
        spdlog::trace("[provider:{}] Successfully created resource {} of type {}",
                id(), resource_id.to_string(), resource_type);
    }

    void openResource(const tl::request& req,
                      const std::string& token,
                      const std::string& resource_type,
                      const std::string& resource_config) {

        spdlog::trace("[provider:{}] Received openResource request", id());
        spdlog::trace("[provider:{}]    => type = {}", id(), resource_type);
        spdlog::trace("[provider:{}]    => config = {}", id(), resource_config);

        auto resource_id = UUID::generate();
        RequestResult<UUID> result;

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            req.respond(result);
            spdlog::error("[provider:{}] Invalid security token {}", id(), token);
            return;
        }

        json json_config;
        try {
            json_config = json::parse(resource_config);
        } catch(json::parse_error& e) {
            result.error() = e.what();
            result.success() = false;
            spdlog::error("[provider:{}] Could not parse resource configuration for resource {}",
                    id(), resource_id.to_string());
            req.respond(result);
            return;
        }

        std::unique_ptr<Backend> backend;
        try {
            backend = ResourceFactory::openResource(resource_type, get_engine(), json_config);
        } catch(const std::exception& ex) {
            result.success() = false;
            result.error() = ex.what();
            spdlog::error("[provider:{}] Error when opening resource {} of type {}:",
                    id(), resource_id.to_string(), resource_type);
            spdlog::error("[provider:{}]    => {}", id(), result.error());
            req.respond(result);
            return;
        }

        if(not backend) {
            result.success() = false;
            result.error() = "Unknown resource type "s + resource_type;
            spdlog::error("[provider:{}] Unknown resource type {} for resource {}",
                    id(), resource_type, resource_id.to_string());
            req.respond(result);
            return;
        } else {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);
            m_backends[resource_id] = std::move(backend);
            result.value() = resource_id;
        }
        
        req.respond(result);
        spdlog::trace("[provider:{}] Successfully created resource {} of type {}",
                id(), resource_id.to_string(), resource_type);
    }

    void closeResource(const tl::request& req,
                        const std::string& token,
                        const UUID& resource_id) {
        spdlog::trace("[provider:{}] Received closeResource request for resource {}",
                id(), resource_id.to_string());

        RequestResult<bool> result;

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            req.respond(result);
            spdlog::error("[provider:{}] Invalid security token {}", id(), token);
            return;
        }

        {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);

            if(m_backends.count(resource_id) == 0) {
                result.success() = false;
                result.error() = "Resource "s + resource_id.to_string() + " not found";
                req.respond(result);
                spdlog::error("[provider:{}] Resource {} not found", id(), resource_id.to_string());
                return;
            }

            m_backends.erase(resource_id);
        }
        req.respond(result);
        spdlog::trace("[provider:{}] Resource {} successfully closed", id(), resource_id.to_string());
    }
    
    void destroyResource(const tl::request& req,
                         const std::string& token,
                         const UUID& resource_id) {
        RequestResult<bool> result;
        spdlog::trace("[provider:{}] Received destroyResource request for resource {}", id(), resource_id.to_string());

        if(m_token.size() > 0 && m_token != token) {
            result.success() = false;
            result.error() = "Invalid security token";
            req.respond(result);
            spdlog::error("[provider:{}] Invalid security token {}", id(), token);
            return;
        }

        {
            std::lock_guard<tl::mutex> lock(m_backends_mtx);

            if(m_backends.count(resource_id) == 0) {
                result.success() = false;
                result.error() = "Resource "s + resource_id.to_string() + " not found";
                req.respond(result);
                spdlog::error("[provider:{}] Resource {} not found", id(), resource_id.to_string());
                return;
            }

            result = m_backends[resource_id]->destroy();
            m_backends.erase(resource_id);
        }

        req.respond(result);
        spdlog::trace("[provider:{}] Resource {} successfully destroyed", id(), resource_id.to_string());
    }

    void checkResource(const tl::request& req,
                       const UUID& resource_id) {
        spdlog::trace("[provider:{}] Received checkResource request for resource {}", id(), resource_id.to_string());
        RequestResult<bool> result;
        FIND_RESOURCE(resource);
        result.success() = true;
        req.respond(result);
        spdlog::trace("[provider:{}] Code successfully executed on resource {}", id(), resource_id.to_string());
    }

    void sayHello(const tl::request& req,
                  const UUID& resource_id) {
        spdlog::trace("[provider:{}] Received sayHello request for resource {}", id(), resource_id.to_string());
        RequestResult<bool> result;
        FIND_RESOURCE(resource);
        resource->sayHello();
        spdlog::trace("[provider:{}] Successfully executed sayHello on resource {}", id(), resource_id.to_string());
    }

    void computeSum(const tl::request& req,
                    const UUID& resource_id,
                    int32_t x, int32_t y) {
        spdlog::trace("[provider:{}] Received sayHello request for resource {}", id(), resource_id.to_string());
        RequestResult<int32_t> result;
        FIND_RESOURCE(resource);
        result = resource->computeSum(x, y);
        req.respond(result);
        spdlog::trace("[provider:{}] Successfully executed computeSum on resource {}", id(), resource_id.to_string());
    }

};

}

#endif
