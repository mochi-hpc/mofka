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
#include "TopicHandleImpl.hpp"
#include "ConsumerImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <utility>
#include <tuple>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Client);

Client::Client(const thallium::engine& engine)
: self(std::make_shared<ClientImpl>(engine)) {}

Client::Client(margo_instance_id mid)
: self(std::make_shared<ClientImpl>(thallium::engine{mid})) {}

const thallium::engine& Client::engine() const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    return self->m_engine;
}

std::pair<std::string, uint16_t> discoverMofkaServiceMaster(
        const bedrock::ServiceGroupHandle& bsgh) {
    std::string configs;
    constexpr const char* script = R"(
        $result = [];
        foreach($__config__.providers as $p) {
            if($p.type != "yokan") continue;
            foreach($p.tags as $tag) {
                if($tag != "mofka:master") continue;
                array_push($result, $p.provider_id);
            }
        }
        return $result;
    )";
    bsgh.queryConfig(script, &configs);
    auto doc = Metadata{configs};
    std::vector<std::pair<std::string, uint16_t>> masters;
    for(const auto& p : doc.json().items()) {
        const auto& address = p.key();
        for(const auto& provider_id : p.value()) {
            masters.push_back({address, provider_id.get<uint16_t>()});
        }
    }
    if(masters.empty())
        throw Exception{"Could not find a Yokan provider with the \"mofka:master\" tag"};
    // note: if multiple yokan databases have the mofka:master tag,
    // it is assume that they are linked together via RAFT to replicate the same content
    return masters[0];
}

template<typename T>
static auto makeServiceHandle(std::shared_ptr<ClientImpl> self, const T& ssgArg) {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    try {
        auto bsgh = self->m_bedrock_client.makeServiceGroupHandle(ssgArg);
        auto master = discoverMofkaServiceMaster(bsgh);
        return std::make_shared<ServiceHandleImpl>(self, std::move(bsgh), master);
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

ServiceHandle Client::connect(SSGFileName ssgfile) const {
    return makeServiceHandle(self, std::string{ssgfile});
}

ServiceHandle Client::connect(SSGGroupID gid) const {
    return makeServiceHandle(self, gid.value);
}

const Metadata& Client::getConfig() const {
    if(!self) throw Exception("Uninitialized ServiceHandle instance");
    // TODO
    static Metadata config{"{}"};
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
