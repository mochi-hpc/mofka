/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Client.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Result.hpp"

#include "PimplUtil.hpp"
#include "ClientImpl.hpp"
#include "TopicHandleImpl.hpp"
#include "ConsumerImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <utility>
#include <tuple>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Client);

Client::Client(const thallium::engine& engine)
: self(std::make_shared<ClientImpl>(engine)) {}

const thallium::engine& Client::engine() const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    return self->m_engine;
}

std::vector<PartitionTargetInfo> ClientImpl::discoverMofkaTargets(
        const tl::engine& engine,
        const bedrock::ServiceGroupHandle bsgh) {
    std::string configs;
    bsgh.getConfig(&configs);
    rapidjson::Document doc;
    doc.Parse(configs.c_str(), configs.size());
    std::vector<PartitionTargetInfo> mofka_targets;
    for(auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        const auto& address = it->name.GetString();
        const auto endpoint = engine.lookup(address);
        const auto& config  = it->value;
        const auto& providers = config["providers"];
        for(auto& provider : providers.GetArray()) {
            if(provider["type"] != "mofka") continue;
            uint16_t provider_id = provider["provider_id"].GetInt();
            auto uuid = UUID::from_string(provider["config"]["uuid"].GetString());
            auto target_impl = std::make_shared<PartitionTargetInfoImpl>(
                uuid, tl::provider_handle(endpoint, provider_id));
            mofka_targets.emplace_back(PartitionTargetInfo(target_impl));
        }
    }
    return mofka_targets;
}

ServiceHandle Client::connect(SSGFileName ssgfile) const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    try {
        auto bsgh = self->m_bedrock_client.makeServiceGroupHandle(std::string{ssgfile});
        auto mofka_phs = ClientImpl::discoverMofkaTargets(self->m_engine, bsgh);
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
        auto mofka_targets = ClientImpl::discoverMofkaTargets(self->m_engine, bsgh);
        auto service_handle_impl = std::make_shared<ServiceHandleImpl>(
            self, std::move(bsgh), std::move(mofka_targets));
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

void ClientImpl::forwardBatchToConsumer(
        const thallium::request& req,
        intptr_t consumer_ctx,
        size_t target_info_index,
        size_t count,
        EventID firstID,
        const BulkRef &metadata_sizes,
        const BulkRef &metadata,
        const BulkRef &data_desc_sizes,
        const BulkRef &data_desc) {
    Result<void> result;
    ConsumerImpl* consumer_impl = reinterpret_cast<ConsumerImpl*>(consumer_ctx);
    consumer_impl->recvBatch(target_info_index, count, firstID, metadata_sizes, metadata, data_desc_sizes, data_desc);
    req.respond(result);
}

}
