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

TopicHandle Client::makeTopicHandle(
        std::string_view address,
        uint16_t provider_id,
        const UUID& topic_id,
        bool check) const {
    auto endpoint  = self->m_engine.lookup(std::string{address});
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result;
    result.success() = true;
    if(check) {
        result = self->m_check_topic.on(ph)(topic_id);
    }
    if(result.success()) {
        auto topic_impl = std::make_shared<TopicHandleImpl>(self, std::move(ph), topic_id);
        return TopicHandle(topic_impl);
    } else {
        throw Exception(result.error());
        return TopicHandle(nullptr);
    }
}

const rapidjson::Value& Client::getConfig() const {
    // TODO
    static rapidjson::Value config;
    return config;
}

}
