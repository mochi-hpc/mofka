/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_EVENT_IMPL_H
#define MOFKA_KAFKA_EVENT_IMPL_H

#include "mofka/Event.hpp"
#include "KafkaPartitionInfo.hpp"

#include <librdkafka/rdkafka.h>

namespace mofka {

class KafkaConsumer;

class KafkaEvent : public EventInterface {

    friend class Event;

    public:

    KafkaEvent()
    : m_id{NoMoreEvents}
    {}

    KafkaEvent(EventID id,
               std::shared_ptr<KafkaPartitionInfo> partition,
               Metadata metadata,
               Data data,
               std::shared_ptr<KafkaConsumer> consumer)
    : m_id(std::move(id))
    , m_partition(std::move(partition))
    , m_metadata{std::move(metadata)}
    , m_data{std::move(data)}
    , m_consumer{std::move(consumer)}
    {}

    void acknowledge() const override;

    PartitionInfo partition() const override {
        if(m_partition)
            return m_partition->toPartitionInfo();
        else
            return PartitionInfo{};
    }

    Metadata metadata() const override {
        return m_metadata;
    }

    Data data() const override {
        return m_data;
    }

    EventID id() const override {
        return m_id;
    }

    private:

    EventID                             m_id;
    std::shared_ptr<KafkaPartitionInfo> m_partition;
    Metadata                            m_metadata;
    Data                                m_data;
    std::weak_ptr<KafkaConsumer>        m_consumer;
};

}

#endif
