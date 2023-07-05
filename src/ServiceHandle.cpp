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

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(ServiceHandle);

Client ServiceHandle::client() const {
    return Client(self->m_client);
}

TopicHandle ServiceHandle::createTopic(
        std::string_view name, std::string_view config, std::string_view type) {
    const auto hash = std::hash<decltype(name)>()(name);
    const auto ph   = self->m_mofka_phs[hash % self->m_mofka_phs.size()];
    RequestResult<UUID> response =
        self->m_client->m_create_topic.on(ph)(
            std::string{name},
            std::string{config},
            std::string{type});
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
