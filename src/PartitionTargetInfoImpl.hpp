/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_TARGET_INFO_IMPL_H
#define MOFKA_PARTITION_TARGET_INFO_IMPL_H

#include "mofka/TargetSelector.hpp"

namespace mofka {

class PartitionTargetInfoImpl {

    public:

    PartitionTargetInfoImpl(UUID uuid, std::string address, uint16_t provider_id)
    : m_uuid(uuid)
    , m_address(std::move(address))
    , m_provider_id(provider_id) {}

    UUID        m_uuid;
    std::string m_address;
    uint16_t    m_provider_id;
};

}

#endif
