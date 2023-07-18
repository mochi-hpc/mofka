/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/ServiceHandle.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"

#include "PimplUtil.hpp"
#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "ServiceHandleImpl.hpp"
#include "TopicHandleImpl.hpp"
#include "MetadataImpl.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(ServiceHandle);

Client ServiceHandle::client() const {
    return Client(self->m_client);
}

TopicHandle ServiceHandle::createTopic(
        std::string_view name,
        TopicBackendConfig config,
        Validator validator,
        TargetSelector selector,
        Serializer serializer) {
    const auto hash   = std::hash<decltype(name)>()(name);
    const auto target = self->m_mofka_targets[hash % self->m_mofka_targets.size()];
    const auto ph = target.self->m_ph;
    using ResultType = std::tuple<Metadata, Metadata, Metadata>;
    RequestResult<ResultType> response =
        self->m_client->m_create_topic.on(ph)(
            std::string{name},
            static_cast<Metadata&>(config),
            validator.metadata(),
            selector.metadata(),
            serializer.metadata());
    if(!response.success())
        throw Exception(response.error());

    Metadata validator_meta;
    Metadata selector_meta;
    Metadata serializer_meta;
    std::tie(validator_meta, selector_meta, serializer_meta) = response.value();
    validator = Validator::FromMetadata(validator_meta);
    selector = TargetSelector::FromMetadata(selector_meta);
    serializer = Serializer::FromMetadata(serializer_meta);
    return std::make_shared<TopicHandleImpl>(
        name, self,
        std::move(validator),
        std::move(selector),
        std::move(serializer));
}

TopicHandle ServiceHandle::openTopic(std::string_view name) {
    const auto hash = std::hash<decltype(name)>()(name);
    const auto target = self->m_mofka_targets[hash % self->m_mofka_targets.size()];
    const auto ph = target.self->m_ph;
    using ResultType = std::tuple<Metadata, Metadata, Metadata>;
    RequestResult<ResultType> response =
        self->m_client->m_open_topic.on(ph)(std::string{name});
    if(!response.success())
        throw Exception(response.error());

    Metadata validator_meta;
    Metadata selector_meta;
    Metadata serializer_meta;
    std::tie(validator_meta, selector_meta, serializer_meta) = response.value();
    auto validator = Validator::FromMetadata(validator_meta);
    auto selector = TargetSelector::FromMetadata(selector_meta);
    auto serializer = Serializer::FromMetadata(serializer_meta);
    return std::make_shared<TopicHandleImpl>(
        name, self,
        std::move(validator),
        std::move(selector),
        std::move(serializer));
}

}
