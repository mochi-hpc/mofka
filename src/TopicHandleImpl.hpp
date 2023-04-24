/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "ServiceHandleImpl.hpp"
#include "mofka/UUID.hpp"

namespace mofka {

class TopicHandleImpl {

    public:

    std::shared_ptr<ServiceHandleImpl> m_service;
    UUID                               m_topic_id;

    TopicHandleImpl() = default;

    TopicHandleImpl(std::shared_ptr<ServiceHandleImpl> service, UUID topic_id)
    : m_service(service)
    , m_topic_id(topic_id) {}
};

}

#endif
