/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_INFO_IMPL_H
#define MOFKA_PARTITION_INFO_IMPL_H

#include "mofka/PartitionSelector.hpp"
#include <thallium.hpp>

namespace mofka {

class PartitionInfoImpl {

    public:

    PartitionInfoImpl(UUID uuid, thallium::provider_handle ph)
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
struct hash<mofka::PartitionInfo> {

    std::size_t operator()(const mofka::PartitionInfo& p) const noexcept {
        return std::hash<std::shared_ptr<mofka::PartitionInfoImpl>>()(p.self);
    }

};

}

#endif
