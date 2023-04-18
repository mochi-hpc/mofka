/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOFKA_TOPIC_HANDLE_IMPL_H
#define __MOFKA_TOPIC_HANDLE_IMPL_H

#include <mofka/UUID.hpp>

namespace mofka {

class TopicHandleImpl {

    public:

    UUID                        m_topic_id;
    std::shared_ptr<ClientImpl> m_client;
    tl::provider_handle         m_ph;

    TopicHandleImpl() = default;
    
    TopicHandleImpl(const std::shared_ptr<ClientImpl>& client, 
                       tl::provider_handle&& ph,
                       const UUID& topic_id)
    : m_topic_id(topic_id)
    , m_client(client)
    , m_ph(std::move(ph)) {}
};

}

#endif
