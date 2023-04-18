/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "alpha/ResourceHandle.hpp"
#include "alpha/RequestResult.hpp"
#include "alpha/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "ResourceHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace alpha {

ResourceHandle::ResourceHandle() = default;

ResourceHandle::ResourceHandle(const std::shared_ptr<ResourceHandleImpl>& impl)
: self(impl) {}

ResourceHandle::ResourceHandle(const ResourceHandle&) = default;

ResourceHandle::ResourceHandle(ResourceHandle&&) = default;

ResourceHandle& ResourceHandle::operator=(const ResourceHandle&) = default;

ResourceHandle& ResourceHandle::operator=(ResourceHandle&&) = default;

ResourceHandle::~ResourceHandle() = default;

ResourceHandle::operator bool() const {
    return static_cast<bool>(self);
}

Client ResourceHandle::client() const {
    return Client(self->m_client);
}

void ResourceHandle::sayHello() const {
    if(not self) throw Exception("Invalid alpha::ResourceHandle object");
    auto& rpc = self->m_client->m_say_hello;
    auto& ph  = self->m_ph;
    auto& resource_id = self->m_resource_id;
    rpc.on(ph)(resource_id);
}

void ResourceHandle::computeSum(
        int32_t x, int32_t y,
        int32_t* result,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid alpha::ResourceHandle object");
    auto& rpc = self->m_client->m_compute_sum;
    auto& ph  = self->m_ph;
    auto& resource_id = self->m_resource_id;
    if(req == nullptr) { // synchronous call
        RequestResult<int32_t> response = rpc.on(ph)(resource_id, x, y);
        if(response.success()) {
            if(result) *result = response.value();
        } else {
            throw Exception(response.error());
        }
    } else { // asynchronous call
        auto async_response = rpc.on(ph).async(resource_id, x, y);
        auto async_request_impl =
            std::make_shared<AsyncRequestImpl>(std::move(async_response));
        async_request_impl->m_wait_callback =
            [result](AsyncRequestImpl& async_request_impl) {
                RequestResult<int32_t> response =
                    async_request_impl.m_async_response.wait();
                    if(response.success()) {
                        if(result) *result = response.value();
                    } else {
                        throw Exception(response.error());
                    }
            };
        *req = AsyncRequest(std::move(async_request_impl));
    }
}

}
