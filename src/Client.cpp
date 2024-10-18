/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Client.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Result.hpp"

#include "JsonUtil.hpp"
#include "PimplUtil.hpp"
#include "ClientImpl.hpp"
#include "MofkaDriverImpl.hpp"

#include <bedrock/ServiceGroupHandle.hpp>

#include <nlohmann/json.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <fstream>
#include <utility>
#include <tuple>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Client);

Client::Client(const thallium::engine& engine)
: self(std::make_shared<ClientImpl>(engine)) {}

Client::Client(margo_instance_id mid)
: self(std::make_shared<ClientImpl>(thallium::engine{mid})) {}

const thallium::engine& Client::engine() const {
    if(!self) throw Exception("Uninitialized MofkaDriver instance");
    return self->m_engine;
}

MofkaDriver Client::connect(const std::string& groupfile) const {
    if(!self) throw Exception("Uninitialized Client instance");
    return MofkaDriver{groupfile, self->m_engine};
}

const Metadata& Client::getConfig() const {
    if(!self) throw Exception("Uninitialized MofkaDriver instance");
    static Metadata config{"{}"};
    return config;
}

}
