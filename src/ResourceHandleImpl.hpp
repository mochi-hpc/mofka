/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_RESOURCE_HANDLE_IMPL_H
#define __ALPHA_RESOURCE_HANDLE_IMPL_H

#include <alpha/UUID.hpp>

namespace alpha {

class ResourceHandleImpl {

    public:

    UUID                        m_resource_id;
    std::shared_ptr<ClientImpl> m_client;
    tl::provider_handle         m_ph;

    ResourceHandleImpl() = default;
    
    ResourceHandleImpl(const std::shared_ptr<ClientImpl>& client, 
                       tl::provider_handle&& ph,
                       const UUID& resource_id)
    : m_resource_id(resource_id)
    , m_client(client)
    , m_ph(std::move(ph)) {}
};

}

#endif
