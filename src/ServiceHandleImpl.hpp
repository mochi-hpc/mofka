/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_IMPL_H
#define MOFKA_SERVICE_HANDLE_IMPL_H

#include "ClientImpl.hpp"

namespace mofka {

class ServiceHandleImpl {

    public:

    std::shared_ptr<ClientImpl> m_client;

    ServiceHandleImpl() = default;

    ServiceHandleImpl(const std::shared_ptr<ClientImpl>& client)
    : m_client(client) {}
};

}

#endif
