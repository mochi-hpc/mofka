/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_IMPL_H
#define MOFKA_SERVICE_HANDLE_IMPL_H

#include "PimplUtil.hpp"
#include "ClientImpl.hpp"

#include <yokan/cxx/database.hpp>
#include <yokan/cxx/collection.hpp>
#include <yokan/cxx/client.hpp>
#include <bedrock/ServiceGroupHandle.hpp>
#include <thallium.hpp>
#include <vector>

namespace mofka {

class ServiceHandleImpl {

    public:

    SP<ClientImpl>              m_client;
    bedrock::ServiceGroupHandle m_bsgh;

    yokan::Client   m_yk_client;
    yokan::Database m_yk_master_db;

    ServiceHandleImpl(
        std::shared_ptr<ClientImpl> client,
        bedrock::ServiceGroupHandle bsgh,
        const std::pair<std::string, uint16_t>& masterDbInfo)
    : m_client(std::move(client))
    , m_bsgh(std::move(bsgh))
    , m_yk_client{m_client->m_engine.get_margo_instance()}
    , m_yk_master_db{
        m_yk_client.makeDatabaseHandle(
            m_client->m_engine.lookup(masterDbInfo.first).get_addr(),
            masterDbInfo.second)}
    {}
};

}

#endif
