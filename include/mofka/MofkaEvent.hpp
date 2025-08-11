/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_IMPL_H
#define MOFKA_EVENT_IMPL_H

#include <mofka/MofkaPartitionInfo.hpp>

#include <diaspora/Event.hpp>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/vector.hpp>

namespace mofka {

class MofkaEvent : public diaspora::EventInterface {

    friend class Event;

    public:

    MofkaEvent()
    : m_id{diaspora::NoMoreEvents}
    {}

    MofkaEvent(diaspora::EventID id,
               std::shared_ptr<MofkaPartitionInfo> partition,
               diaspora::Metadata metadata,
               diaspora::DataView data,
               std::string consumer_name,
               thallium::remote_procedure ack_rpc)
    : m_id(std::move(id))
    , m_partition(std::move(partition))
    , m_metadata{std::move(metadata)}
    , m_data{std::move(data)}
    , m_consumer_name{std::move(consumer_name)}
    , m_acknowledge_rpc{std::move(ack_rpc)}
    {}

    void acknowledge() const override {
        using namespace std::string_literals;
        if(m_id == diaspora::NoMoreEvents)
            throw diaspora::Exception{"Cannot acknowledge \"NoMoreEvents\""};
        try {
            auto ph = m_partition->m_ph;
            m_acknowledge_rpc.on(ph)(m_consumer_name, m_id);
        } catch(const std::exception& ex) {
            throw diaspora::Exception{"Could not acknowledge event: "s + ex.what()};
        }
    }

    diaspora::PartitionInfo partition() const override {
        if(m_partition)
            return m_partition->toPartitionInfo();
        else
            return diaspora::PartitionInfo{};
    }

    const diaspora::Metadata& metadata() const override {
        return m_metadata;
    }

    const diaspora::DataView& data() const override {
        return m_data;
    }

    diaspora::EventID id() const override {
        return m_id;
    }

    private:

    diaspora::EventID                   m_id;
    std::shared_ptr<MofkaPartitionInfo> m_partition;
    diaspora::Metadata                  m_metadata;
    diaspora::DataView                  m_data;

    std::string                m_consumer_name;
    thallium::remote_procedure m_acknowledge_rpc;
};

}

#endif
