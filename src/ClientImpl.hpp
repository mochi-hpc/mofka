/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CLIENT_IMPL_H
#define MOFKA_CLIENT_IMPL_H

#include "mofka/PartitionSelector.hpp"
#include "mofka/Result.hpp"
#include "mofka/BulkRef.hpp"
#include "mofka/EventID.hpp"
#include <bedrock/Client.hpp>
#include <thallium.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace mofka {

class ConsumerImpl;

namespace tl = thallium;

class ClientImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_get_uuid;
    tl::remote_procedure m_producer_send_batch;
    tl::remote_procedure m_topic_mark_as_complete;

    bedrock::Client      m_bedrock_client;

    ClientImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_producer_send_batch(m_engine.define("mofka_producer_send_batch"))
    , m_topic_mark_as_complete(m_engine.define("mofka_topic_mark_as_complete"))
    , m_bedrock_client(m_engine)
    {}
};

}

#endif
