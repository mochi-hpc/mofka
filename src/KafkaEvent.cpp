/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "KafkaEvent.hpp"
#include "KafkaConsumer.hpp"

namespace mofka {

void KafkaEvent::acknowledge() const {
    using namespace std::string_literals;
    if(m_id == NoMoreEvents)
        throw Exception{"Cannot acknowledge \"NoMoreEvents\""};
    auto consumer = m_consumer.lock();
    if(!consumer)
        throw Exception{"Consumer owning this Event got out of scope"};

    rd_kafka_topic_partition_t partition;
    std::memset(&partition, 0, sizeof(partition));
    partition.topic     = const_cast<char*>(consumer->m_topic->m_name.c_str());
    partition.partition = m_partition->m_id;
    partition.offset    = m_id+1;
        rd_kafka_topic_partition_list_t list = {1, 1, &partition};
    auto err = rd_kafka_commit(consumer->m_kafka_consumer.get(), &list, 1);
    if(err) throw Exception{std::string{"Failed to commit offset: "} + rd_kafka_err2str(err)};
}

}
