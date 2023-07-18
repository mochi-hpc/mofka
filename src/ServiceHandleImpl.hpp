/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_IMPL_H
#define MOFKA_SERVICE_HANDLE_IMPL_H

#include "ClientImpl.hpp"
#include <bedrock/ServiceGroupHandle.hpp>
#include <thallium.hpp>
#include <vector>

namespace mofka {

class ServiceHandleImpl {

    public:

    std::shared_ptr<ClientImpl>      m_client;
    bedrock::ServiceGroupHandle      m_bsgh;
    std::vector<PartitionTargetInfo> m_mofka_targets;

    ServiceHandleImpl(
        std::shared_ptr<ClientImpl> client,
        bedrock::ServiceGroupHandle bsgh,
        std::vector<PartitionTargetInfo> targets)
    : m_client(std::move(client))
    , m_bsgh(std::move(bsgh))
    , m_mofka_targets(std::move(targets)) {}
};

}

#endif
