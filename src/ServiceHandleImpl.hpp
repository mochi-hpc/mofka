/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_IMPL_H
#define MOFKA_SERVICE_HANDLE_IMPL_H

#include "PimplUtil.hpp"
#include "mofka/Client.hpp"

#include <yokan/cxx/database.hpp>
#include <yokan/cxx/collection.hpp>
#include <yokan/cxx/client.hpp>
#include <bedrock/ServiceGroupHandle.hpp>
#include <thallium.hpp>
#include <vector>

namespace mofka {

class ServiceHandleImpl {

    public:

    thallium::engine            m_engine;
    bedrock::ServiceGroupHandle m_bsgh;

    yokan::Client   m_yk_client;
    yokan::Database m_yk_master_db;

    ServiceHandleImpl(
        thallium::engine engine,
        bedrock::ServiceGroupHandle bsgh,
        const std::pair<std::string, uint16_t>& masterDbInfo)
    : m_engine(std::move(engine))
    , m_bsgh(std::move(bsgh))
    , m_yk_client{m_engine.get_margo_instance()}
    , m_yk_master_db{
        m_yk_client.makeDatabaseHandle(
            m_engine.lookup(masterDbInfo.first).get_addr(),
            masterDbInfo.second)}
    {}
};

}

#endif
