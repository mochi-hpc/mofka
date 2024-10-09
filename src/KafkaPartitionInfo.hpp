/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_PARTITION_INFO_IMPL_H
#define MOFKA_KAFKA_PARTITION_INFO_IMPL_H

#include "mofka/PartitionSelector.hpp"

namespace mofka {

class KafkaPartitionInfo {

    public:

    KafkaPartitionInfo() = default;

    KafkaPartitionInfo(int32_t id, int32_t leader, std::vector<int32_t> replicas)
    : m_id(id)
    , m_leader(leader)
    , m_replicas(std::move(replicas))
    {}

    int32_t              m_id;
    int32_t              m_leader;
    std::vector<int32_t> m_replicas;

    PartitionInfo toPartitionInfo() const {
        PartitionInfo info;
        info.json()["id"]       = m_id;
        info.json()["leader"]   = m_leader;
        info.json()["replicas"] = m_replicas;
        return info;
    }
};

}

#endif
