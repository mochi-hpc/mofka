/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "alpha/Admin.hpp"
#include "alpha/Exception.hpp"
#include "alpha/RequestResult.hpp"

#include "AdminImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace alpha {

Admin::Admin() = default;

Admin::Admin(const tl::engine& engine)
: self(std::make_shared<AdminImpl>(engine)) {}

Admin::Admin(margo_instance_id mid)
: self(std::make_shared<AdminImpl>(mid)) {}

Admin::Admin(Admin&& other) = default;

Admin& Admin::operator=(Admin&& other) = default;

Admin::Admin(const Admin& other) = default;

Admin& Admin::operator=(const Admin& other) = default;


Admin::~Admin() = default;

Admin::operator bool() const {
    return static_cast<bool>(self);
}

UUID Admin::createResource(const std::string& address,
                           uint16_t provider_id,
                           const std::string& resource_type,
                           const std::string& resource_config,
                           const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<UUID> result = self->m_create_resource.on(ph)(token, resource_type, resource_config);
    if(not result.success()) {
        throw Exception(result.error());
    }
    return result.value();
}

UUID Admin::openResource(const std::string& address,
                         uint16_t provider_id,
                         const std::string& resource_type,
                         const std::string& resource_config,
                         const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<UUID> result = self->m_open_resource.on(ph)(token, resource_type, resource_config);
    if(not result.success()) {
        throw Exception(result.error());
    }
    return result.value();
}

void Admin::closeResource(const std::string& address,
                           uint16_t provider_id,
                           const UUID& resource_id,
                           const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_close_resource.on(ph)(token, resource_id);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

void Admin::destroyResource(const std::string& address,
                            uint16_t provider_id,
                            const UUID& resource_id,
                            const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_destroy_resource.on(ph)(token, resource_id);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

void Admin::shutdownServer(const std::string& address) const {
    auto ep = self->m_engine.lookup(address);
    self->m_engine.shutdown_remote_engine(ep);
}

}
