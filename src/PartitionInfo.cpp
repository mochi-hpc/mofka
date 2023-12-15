/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/PartitionSelector.hpp"
#include "mofka/Exception.hpp"

#include "PartitionInfoImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(PartitionInfo);

const std::string& PartitionInfo::address() const {
    return self->m_addr;
}

uint16_t PartitionInfo::providerID() const {
    return self->m_ph.provider_id();
}

UUID PartitionInfo::uuid() const {
    return self->m_uuid;
}

}
