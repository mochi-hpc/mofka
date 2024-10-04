/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_IMPL_H
#define MOFKA_EVENT_IMPL_H

#include "mofka/Event.hpp"
#include "MofkaPartitionInfo.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/vector.hpp>

namespace mofka {

class EventImpl {

    friend class Event;

    public:

    EventImpl()
    : m_id{NoMoreEvents}
    {}

    EventImpl(EventID id,
              std::shared_ptr<MofkaPartitionInfo> partition,
              Metadata metadata,
              Data data,
              std::string consumer_name,
              thallium::remote_procedure ack_rpc)
    : m_id(std::move(id))
    , m_partition(std::move(partition))
    , m_metadata{std::move(metadata)}
    , m_data{std::move(data)}
    , m_consumer_name{std::move(consumer_name)}
    , m_acknowledge_rpc{std::move(ack_rpc)}
    {}

    void acknowledge() {
        using namespace std::string_literals;
        if(m_id == NoMoreEvents)
            throw Exception{"Cannot acknowledge \"NoMoreEvents\""};
        try {
            auto ph = m_partition->m_ph;
            m_acknowledge_rpc.on(ph)(m_consumer_name, m_id);
        } catch(const std::exception& ex) {
            throw Exception{"Could not acknowledge event: "s + ex.what()};
        }
    }

    auto partition() const {
        if(m_partition)
            return m_partition->toPartitionInfo();
        else
            return PartitionInfo{};
    }

    auto metadata() const {
        return m_metadata;
    }

    auto data() const {
        return m_data;
    }

    auto id() const {
        return m_id;
    }

    private:

    EventID                             m_id;
    std::shared_ptr<MofkaPartitionInfo> m_partition;
    Metadata                            m_metadata;
    Data                                m_data;

    std::string                m_consumer_name;
    thallium::remote_procedure m_acknowledge_rpc;
};

}

#endif
