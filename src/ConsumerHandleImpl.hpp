/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HANDLE_IMPL_H
#define MOFKA_CONSUMER_HANDLE_IMPL_H

#include "UUID.hpp"
#include "ConsumerHandle.hpp"
#include "PartitionManager.hpp"
#include <thallium.hpp>
#include <queue>

namespace mofka {

/**
 * @brief Key used to identify a ConsumerHandleImpl in an std::unordered_map.
 */
struct ConsumerKey {

    const intptr_t    m_consumer_ptr;
    const std::string m_consumer_addresss;
    const size_t      m_partition_index;

    ConsumerKey(intptr_t ptr,
                std::string addr,
                size_t idx)
    : m_consumer_ptr(ptr)
    , m_consumer_addresss(std::move(addr))
    , m_partition_index(idx) {}

    struct Hash {
        std::size_t operator()(const ConsumerKey& c) const {
            return std::hash<intptr_t>{}(c.m_consumer_ptr)
                 ^ std::hash<size_t>{}(c.m_partition_index)
                 ^ std::hash<std::string>{}(c.m_consumer_addresss);
        }
    };

    bool operator==(const ConsumerKey& other) const {
        return m_consumer_ptr      == other.m_consumer_ptr
            && m_consumer_addresss == other.m_consumer_addresss
            && m_partition_index   == other.m_partition_index;
    }

};

class ConsumerHandleImpl {

    public:

    const intptr_t                          m_consumer_ptr;
    const size_t                            m_partition_index;
    const std::string                       m_consumer_name;
    const size_t                            m_max_events;
    const std::shared_ptr<PartitionManager> m_topic_manager;
    const thallium::endpoint                m_consumer_endpoint;
    const thallium::remote_procedure        m_send_batch;
    std::atomic<bool>                       m_should_stop = false;

    size_t m_sent_events = 0;

    ConsumerHandleImpl(
        intptr_t ptr,
        size_t partition_index,
        std::string name,
        size_t max,
        std::shared_ptr<PartitionManager> topic_manager,
        thallium::endpoint endpoint,
        thallium::remote_procedure rpc)
    : m_consumer_ptr(ptr)
    , m_partition_index(partition_index)
    , m_consumer_name(std::move(name))
    , m_max_events(max)
    , m_topic_manager(std::move(topic_manager))
    , m_consumer_endpoint(std::move(endpoint))
    , m_send_batch(std::move(rpc)) {}

    void stop();
};

}

#endif
