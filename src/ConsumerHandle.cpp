/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "ConsumerHandleImpl.hpp"
#include "Promise.hpp"
#include <spdlog/spdlog.h>
#include <limits>

namespace mofka {

const std::string& ConsumerHandle::name() const {
    return self->m_consumer_name;
}

bool ConsumerHandle::shouldStop() const {
    return self->m_should_stop;
}

diaspora::Future<void> ConsumerHandle::feed(
    size_t count,
    diaspora::EventID firstID,
    const BulkRef &metadata_sizes,
    const BulkRef &metadata,
    const BulkRef &data_desc_sizes,
    const BulkRef &data_desc)
{
    try {
        auto request = self->m_send_batch.on(self->m_consumer_endpoint).async(
            self->m_consumer_ptr,
            self->m_partition_index,
            count,
            firstID,
            metadata_sizes,
            metadata,
            data_desc_sizes,
            data_desc);
        auto req_ptr = std::make_shared<thallium::async_response>(std::move(request));
        return diaspora::Future<void>{
            [req_ptr]() {
                req_ptr->wait();
            },
            [req_ptr]() {
                return req_ptr->received();
            }
        };
    } catch(const std::exception& ex) {
        return diaspora::Future<void>{
            [ex](){ throw ex; },
            [](){ return true; }
        };
    }
}

void ConsumerHandleImpl::stop() {
    m_should_stop = true;
    m_topic_manager->wakeUp();
}

}
