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

static std::vector<tl::provider_handle> discoverMofkaProviders(
        const tl::engine& engine, const bedrock::ServiceGroupHandle bsgh) {
    std::string configs;
    bsgh.getConfig(&configs);
    rapidjson::Document doc;
    doc.Parse(configs.c_str(), configs.size());
    std::vector<tl::provider_handle> mofka_phs;
    for(auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        const auto& address = it->name.GetString();
        const auto endpoint = engine.lookup(address);
        const auto& config  = it->value;
        const auto& providers = config["providers"];
        for(auto& provider : providers.GetArray()) {
            if(provider["type"] != "mofka") continue;
            uint16_t provider_id = provider["provider_id"].GetInt();
            mofka_phs.emplace_back(endpoint, provider_id);
        }
    }
    return mofka_phs;
}

ServiceHandle Client::connect(SSGFileName ssgfile) const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    try {
        auto bsgh = self->m_bedrock_client.makeServiceGroupHandle(std::string{ssgfile});
        auto mofka_phs = discoverMofkaProviders(self->m_engine, bsgh);
        auto service_handle_impl = std::make_shared<ServiceHandleImpl>(
            self, std::move(bsgh), std::move(mofka_phs));
        return ServiceHandle(std::move(service_handle_impl));
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

ServiceHandle Client::connect(SSGGroupID gid) const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    try {
        auto bsgh = self->m_bedrock_client.makeServiceGroupHandle(gid.value);
        auto mofka_phs = discoverMofkaProviders(self->m_engine, bsgh);
        auto service_handle_impl = std::make_shared<ServiceHandleImpl>(
            self, std::move(bsgh), std::move(mofka_phs));
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
