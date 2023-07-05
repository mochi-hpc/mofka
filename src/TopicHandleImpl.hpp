/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "ServiceHandleImpl.hpp"
#include "mofka/UUID.hpp"
#include <string_view>

namespace mofka {

class TopicHandleImpl {

    public:

    std::string                        m_name;
    std::shared_ptr<ServiceHandleImpl> m_service;
    UUID                               m_topic_id;

    TopicHandleImpl() = default;

    TopicHandleImpl(std::string_view name,
                    std::shared_ptr<ServiceHandleImpl> service,
                    UUID topic_id)
    : m_name(name)
    , m_service(std::move(service))
    , m_topic_id(topic_id) {}
};

}

#endif
