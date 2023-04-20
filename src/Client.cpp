/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Client.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "TopicHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace mofka {

Client::Client() = default;

Client::Client(const thallium::engine& engine)
: self(std::make_shared<ClientImpl>(engine)) {}

Client::Client(const std::shared_ptr<ClientImpl>& impl)
: self(impl) {}

Client::Client(Client&& other) = default;

Client& Client::operator=(Client&& other) = default;

Client::Client(const Client& other) = default;

Client& Client::operator=(const Client& other) = default;

Client::~Client() = default;

const thallium::engine& Client::engine() const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    return self->m_engine;
}

Client::operator bool() const {
    return static_cast<bool>(self);
}

ServiceHandle Client::connect(SSGFileName ssgfile) const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    try {
        auto bsgh = self->m_bedrock_client.makeServiceGroupHandle(std::string{ssgfile});
        auto service_handle_impl = std::make_shared<ServiceHandleImpl>(self, std::move(bsgh));
        return ServiceHandle(std::move(service_handle_impl));
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

ServiceHandle Client::connect(SSGGroupID gid) const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    try {
        auto bsgh = self->m_bedrock_client.makeServiceGroupHandle(gid.value);
        auto service_handle_impl = std::make_shared<ServiceHandleImpl>(self, std::move(bsgh));
        return ServiceHandle(std::move(service_handle_impl));
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

const rapidjson::Value& Client::getConfig() const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    // TODO
    static rapidjson::Value config;
    return config;
}

}
