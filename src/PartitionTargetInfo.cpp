/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TargetSelector.hpp"
#include "mofka/Exception.hpp"

#include "PartitionTargetInfoImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(PartitionTargetInfo);

const std::string& PartitionTargetInfo::address() const {
    return self->m_addr;
}

uint16_t PartitionTargetInfo::providerID() const {
    return self->m_ph.provider_id();
}

UUID PartitionTargetInfo::uuid() const {
    return self->m_uuid;
}

}
