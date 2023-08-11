/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "ConsumerHandleImpl.hpp"
#include "PimplUtil.hpp"
#include <spdlog/spdlog.h>
#include <limits>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_DTOR(ConsumerHandle);

ConsumerHandle::~ConsumerHandle() {
}

const std::string& ConsumerHandle::name() const {
    return self->m_consumer_name;
}

bool ConsumerHandle::shouldStop() const {
    return self->m_should_stop;
}

void ConsumerHandle::feed(
    size_t count,
    EventID firstID,
    const BulkRef &metadata_sizes,
    const BulkRef &metadata,
    const BulkRef &data_desc_sizes,
    const BulkRef &data_desc)
{
    try {
        self->m_send_batch.on(self->m_consumer_endpoint)(
            self->m_consumer_ctx,
            self->m_target_info_index,
            count,
            firstID,
            metadata_sizes,
            metadata,
            data_desc_sizes,
            data_desc);
    } catch(const std::exception& ex) {
        spdlog::warn("Exception throw will sending batch to consumer: {}", ex.what());
    }
}

void ConsumerHandleImpl::stop() {
    m_should_stop = true;
    m_topic_manager->wakeUp();
}

}
