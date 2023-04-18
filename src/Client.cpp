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
    return self->m_engine;
}

Client::operator bool() const {
    return static_cast<bool>(self);
}

ServiceHandle Client::connect(std::string_view ssgfile) const {
    // TODO
    (void)ssgfile;
    return {};
}

const rapidjson::Value& Client::getConfig() const {
    // TODO
    static rapidjson::Value config;
    return config;
}

}
