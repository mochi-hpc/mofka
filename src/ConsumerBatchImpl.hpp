/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_BATCH_IMPL_H
#define MOFKA_CONSUMER_BATCH_IMPL_H

#include <thallium.hpp>
#include <mutex>
#include <queue>
#include "mofka/BulkRef.hpp"
#include "mofka/Result.hpp"
#include "mofka/EventID.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Data.hpp"
#include "mofka/Future.hpp"
#include "mofka/Consumer.hpp"
#include "PartitionInfoImpl.hpp"
#include "ThreadPoolImpl.hpp"
#include "ConsumerImpl.hpp"
#include "Promise.hpp"
#include "DataImpl.hpp"
#include <vector>
#include <cstdint>

namespace mofka {

class ConsumerImpl;

class ConsumerBatchImpl {

    friend class ConsumerImpl;

    thallium::engine    m_engine;
    std::vector<size_t> m_meta_sizes;       /* size of each serialized metadata object */
    std::vector<char>   m_meta_buffer;      /* packed serialized metadata objects */
    std::vector<size_t> m_data_desc_sizes;  /* size of the data descriptors associated with each metadata */
    std::vector<char>   m_data_desc_buffer; /* packed data descriptors */

    public:

    ConsumerBatchImpl(thallium::engine engine, size_t count, size_t metadata_size, size_t data_desc_size)
    : m_engine(std::move(engine))
    , m_meta_sizes(count)
    , m_meta_buffer(metadata_size)
    , m_data_desc_sizes(count)
    , m_data_desc_buffer(data_desc_size) {}

    ConsumerBatchImpl(ConsumerBatchImpl&&) = default;
    ConsumerBatchImpl(const ConsumerBatchImpl&) = delete;
    ConsumerBatchImpl& operator=(ConsumerBatchImpl&&) = default;
    ConsumerBatchImpl& operator=(const ConsumerBatchImpl&) = default;
    ~ConsumerBatchImpl() = default;

    void pullFrom(const BulkRef& remote_meta_sizes,
                  const BulkRef& remote_meta_buffer,
                  const BulkRef& remote_desc_sizes,
                  const BulkRef& remote_desc_buffer) {
        std::vector<std::pair<void*, size_t>> segments = {
            {m_meta_sizes.data(), m_meta_sizes.size()*sizeof(m_meta_sizes[0])},
            {m_meta_buffer.data(), m_meta_buffer.size()*sizeof(m_meta_buffer[0])},
            {m_data_desc_sizes.data(), m_data_desc_sizes.size()*sizeof(m_data_desc_sizes[0])},
            {m_data_desc_buffer.data(), m_data_desc_buffer.size()*sizeof(m_data_desc_buffer[0])}
        };
        auto local_bulk = m_engine.expose(segments, thallium::bulk_mode::write_only);
        size_t local_offset = 0;
        auto pull_bulk_ref = [](thallium::bulk& local,
                                size_t local_offset,
                                thallium::endpoint& ep,
                                const BulkRef& remote) {
            if(remote.size == 0) return;
            local(local_offset, remote.size) << remote.handle.on(ep)(remote.offset, remote.size);
        };
        // transfer metadata sizes
        thallium::endpoint remote_ep = m_engine.lookup(remote_meta_sizes.address);
        pull_bulk_ref(local_bulk, local_offset, remote_ep, remote_meta_sizes);
        local_offset += segments[0].second;
        // transfer metadata
        if(remote_meta_buffer.address != remote_meta_sizes.address)
            remote_ep = m_engine.lookup(remote_meta_buffer.address);
        pull_bulk_ref(local_bulk, local_offset, remote_ep, remote_meta_buffer);
        local_offset += segments[1].second;
        // transfer data descriptor sizes
        if(remote_desc_sizes.address != remote_meta_buffer.address)
            remote_ep = m_engine.lookup(remote_desc_sizes.address);
        pull_bulk_ref(local_bulk, local_offset, remote_ep, remote_desc_sizes);
        local_offset += segments[2].second;
        // transfer data descriptors
        if(remote_desc_buffer.address != remote_desc_sizes.address)
            remote_ep = m_engine.lookup(remote_desc_buffer.address);
        pull_bulk_ref(local_bulk, local_offset, remote_ep, remote_desc_buffer);
    }

    size_t count() const {
        return m_meta_sizes.size();
    }
};

}

#endif
