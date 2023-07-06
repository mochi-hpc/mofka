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
        Serializer serializer) {
    const auto hash = std::hash<decltype(name)>()(name);
    const auto ph   = self->m_mofka_phs[hash % self->m_mofka_phs.size()];
    RequestResult<UUID> response =
        self->m_client->m_create_topic.on(ph)(
            std::string{name},
            static_cast<Metadata&>(config),
            serializer.metadata());
    if(!response.success())
        throw Exception(response.error());
    return std::make_shared<TopicHandleImpl>(name, self, response.value());
}

TopicHandle ServiceHandle::openTopic(std::string_view name) {
    const auto hash = std::hash<decltype(name)>()(name);
    const auto ph   = self->m_mofka_phs[hash % self->m_mofka_phs.size()];
    RequestResult<UUID> response =
        self->m_client->m_open_topic.on(ph)(std::string{name});
    if(!response.success())
        throw Exception(response.error());
    return std::make_shared<TopicHandleImpl>(name, self, response.value());
}

}
