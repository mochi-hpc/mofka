/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_IMPL_H
#define MOFKA_SERVICE_HANDLE_IMPL_H

#include "ClientImpl.hpp"
#include <bedrock/ServiceGroupHandle.hpp>

namespace mofka {

class ServiceHandleImpl {

    public:

    std::shared_ptr<ClientImpl> m_client;
    bedrock::ServiceGroupHandle m_bsgh;

    ServiceHandleImpl(
        std::shared_ptr<ClientImpl> client,
        bedrock::ServiceGroupHandle bsgh)
    : m_client(std::move(client))
    , m_bsgh(std::move(bsgh)) {}
};

}

#endif
