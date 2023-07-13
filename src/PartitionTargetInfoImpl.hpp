/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_TARGET_INFO_IMPL_H
#define MOFKA_PARTITION_TARGET_INFO_IMPL_H

#include "mofka/TargetSelector.hpp"
#include <thallium.hpp>

namespace mofka {

class PartitionTargetInfoImpl {

    public:

    PartitionTargetInfoImpl(UUID uuid, thallium::provider_handle ph)
    : m_uuid(uuid)
    , m_ph(std::move(ph))
    , m_addr(m_ph) {}

    UUID                      m_uuid;
    thallium::provider_handle m_ph;
    std::string               m_addr;
};

}

namespace std {

template<>
struct hash<mofka::PartitionTargetInfo> {

    std::size_t operator()(const mofka::PartitionTargetInfo& p) const noexcept {
        return std::hash<std::shared_ptr<mofka::PartitionTargetInfoImpl>>()(p.self);
    }

};

}

#endif
