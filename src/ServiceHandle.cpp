/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/ServiceHandle.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "ServiceHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

ServiceHandle::ServiceHandle() = default;

ServiceHandle::ServiceHandle(const std::shared_ptr<ServiceHandleImpl>& impl)
: self(impl) {}

ServiceHandle::ServiceHandle(const ServiceHandle&) = default;

ServiceHandle::ServiceHandle(ServiceHandle&&) = default;

ServiceHandle& ServiceHandle::operator=(const ServiceHandle&) = default;

ServiceHandle& ServiceHandle::operator=(ServiceHandle&&) = default;

ServiceHandle::~ServiceHandle() = default;

ServiceHandle::operator bool() const {
    return static_cast<bool>(self);
}

Client ServiceHandle::client() const {
    return Client(self->m_client);
}

}
