/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TopicHandle.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "TopicHandleImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

TopicHandle::TopicHandle() = default;

TopicHandle::TopicHandle(const std::shared_ptr<TopicHandleImpl>& impl)
: self(impl) {}

TopicHandle::TopicHandle(const TopicHandle&) = default;

TopicHandle::TopicHandle(TopicHandle&&) = default;

TopicHandle& TopicHandle::operator=(const TopicHandle&) = default;

TopicHandle& TopicHandle::operator=(TopicHandle&&) = default;

TopicHandle::~TopicHandle() = default;

TopicHandle::operator bool() const {
    return static_cast<bool>(self);
}

ServiceHandle TopicHandle::service() const {
    return ServiceHandle(self->m_service);
}

void TopicHandle::sayHello() const {
    if(not self) throw Exception("Invalid mofka::TopicHandle object");
    /*
    auto& rpc = self->m_service->m_client->m_say_hello;
    auto& ph  = self->m_ph;
    auto& topic_id = self->m_topic_id;
    rpc.on(ph)(topic_id);
    */
}

void TopicHandle::computeSum(
        int32_t x, int32_t y,
        int32_t* result,
        AsyncRequest* req) const
{
    if(not self) throw Exception("Invalid mofka::TopicHandle object");
    /*
    auto& rpc = self->m_client->m_compute_sum;
    auto& ph  = self->m_ph;
    auto& topic_id = self->m_topic_id;
    if(req == nullptr) { // synchronous call
        RequestResult<int32_t> response = rpc.on(ph)(topic_id, x, y);
        if(response.success()) {
            if(result) *result = response.value();
        } else {
            throw Exception(response.error());
        }
    } else { // asynchronous call
        auto async_response = rpc.on(ph).async(topic_id, x, y);
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
    */
}

}
