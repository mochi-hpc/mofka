/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_INFO_IMPL_H
#define MOFKA_PARTITION_INFO_IMPL_H

#include "UUID.hpp"
#include <diaspora/PartitionSelector.hpp>
#include <thallium.hpp>

namespace mofka {

class MofkaPartitionInfo {

    public:

    MofkaPartitionInfo() = default;

    MofkaPartitionInfo(UUID uuid, thallium::provider_handle ph)
    : m_uuid(uuid)
    , m_ph(std::move(ph)) {}

    UUID                      m_uuid;
    thallium::provider_handle m_ph;

    diaspora::PartitionInfo toPartitionInfo() const {
        diaspora::PartitionInfo info;
        info.json()["uuid"] = m_uuid.to_string();
        info.json()["address"] = static_cast<std::string>(m_ph);
        info.json()["provider_id"] = m_ph.provider_id();
        return info;
    }
};

}

#endif
