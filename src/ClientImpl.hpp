/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CLIENT_IMPL_H
#define MOFKA_CLIENT_IMPL_H

#include "mofka/TargetSelector.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/BulkRef.hpp"
#include "mofka/EventID.hpp"
#include <bedrock/Client.hpp>
#include <thallium.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace mofka {

namespace tl = thallium;

class ClientImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_create_topic;
    tl::remote_procedure m_open_topic;
    tl::remote_procedure m_get_uuid;
    tl::remote_procedure m_producer_send_batch;
    tl::remote_procedure m_consumer_request_events;
    tl::remote_procedure m_consumer_ack_event;
    tl::remote_procedure m_consumer_remove_consumer;
    tl::remote_procedure m_consumer_recv_batch;
    bedrock::Client      m_bedrock_client;

    ClientImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_create_topic(m_engine.define("mofka_create_topic"))
    , m_open_topic(m_engine.define("mofka_open_topic"))
    , m_producer_send_batch(m_engine.define("mofka_producer_send_batch"))
    , m_consumer_request_events(m_engine.define("mofka_consumer_request_events"))
    , m_consumer_ack_event(m_engine.define("mofka_consumer_ack_event"))
    , m_consumer_remove_consumer(m_engine.define("mofka_consumer_remove_consumer"))
    , m_consumer_recv_batch(m_engine.define("mofka_consumer_recv_batch", forwardBatchToConsumer))
    , m_bedrock_client(m_engine)
    {}

    static void forwardBatchToConsumer(
            const thallium::request& req,
            intptr_t consumer_ctx,
            size_t count,
            EventID firstID,
            const BulkRef &metadata_sizes,
            const BulkRef &metadata,
            const BulkRef &data_desc_sizes,
            const BulkRef &data_desc);

    static std::vector<PartitionTargetInfo> discoverMofkaTargets(
            const tl::engine& engine,
            const bedrock::ServiceGroupHandle bsgh);
};

}

#endif
