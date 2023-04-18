/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "ServiceHandleImpl.hpp"

namespace mofka {

class TopicHandleImpl {

    public:

    std::shared_ptr<ServiceHandleImpl> m_service;

    TopicHandleImpl() = default;

    TopicHandleImpl(const std::shared_ptr<ServiceHandleImpl>& service)
    : m_service(service) {}
};

}

#endif
