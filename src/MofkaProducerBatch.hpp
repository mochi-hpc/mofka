/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_BATCH_IMPL_H
#define MOFKA_PRODUCER_BATCH_IMPL_H

#include <diaspora/EventID.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Archive.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/Future.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/BufferWrapperArchive.hpp>

#include "MofkaPartitionInfo.hpp"
#include "MofkaProducer.hpp"
#include "Promise.hpp"
#include "ProducerBatchInterface.hpp"
#include "BulkRef.hpp"
#include "Result.hpp"

#include <thallium.hpp>
#include <fmt/format.h>
#include <mutex>
#include <queue>
#include <vector>
#include <cstdint>

namespace mofka {

namespace tl = thallium;

class MofkaProducerBatch : public ProducerBatchInterface {

    struct Entry {
        diaspora::Metadata         metadata;
        diaspora::DataView         data;
        Promise<diaspora::EventID> promise;
    };

    std::string                m_producer_name;
    thallium::engine           m_engine;
    diaspora::Serializer       m_serializer;
    thallium::provider_handle  m_partition_ph;
    thallium::remote_procedure m_send_batch_rpc;

    std::vector<Entry> m_entries;

    /* buffers for serialization and sending */
    std::vector<size_t>                   m_meta_sizes;
    std::vector<char>                     m_meta_buffer;
    std::vector<size_t>                   m_data_sizes;
    std::vector<std::pair<void*, size_t>> m_data_segments;

    thallium::bulk m_meta_bulk;

    public:

    MofkaProducerBatch(
        std::string producer_name,
        thallium::engine engine,
        diaspora::Serializer serializer,
        thallium::provider_handle partition_ph,
        thallium::remote_procedure send_batch)
    : m_producer_name{std::move(producer_name)}
    , m_engine{std::move(engine)}
    , m_serializer{std::move(serializer)}
    , m_partition_ph{std::move(partition_ph)}
    , m_send_batch_rpc{std::move(send_batch)}
    {}

    void push(diaspora::Metadata metadata,
              diaspora::DataView data,
              Promise<diaspora::EventID> promise) override {
        m_entries.push_back({std::move(metadata), std::move(data), std::move(promise)});
    }

    void send() override {

        m_meta_sizes.reserve(count());
        bool first_entry = true;
        diaspora::BufferWrapperOutputArchive archive(m_meta_buffer);
        m_data_segments.emplace_back(); // first entry changed later
        for(auto& entry : m_entries) {
            size_t meta_buffer_size = m_meta_buffer.size();
            m_serializer.serialize(archive, entry.metadata);
            size_t meta_size = m_meta_buffer.size() - meta_buffer_size;
            if(first_entry) {
                // use the first entry metadata size as an estimate for the total size
                size_t estimated_total_size = meta_size * m_entries.size() * 1.1;
                m_meta_buffer.reserve(estimated_total_size);
                first_entry = false;
            }
            m_meta_sizes.push_back(meta_size);
            size_t data_size = 0;
            for(const auto& seg : entry.data.segments()) {
                if(seg.size == 0) continue;
                m_data_segments.emplace_back(seg.ptr, seg.size);
                data_size += seg.size;
            }
            m_data_sizes.push_back(data_size);
        }
        m_data_segments[0] = {m_data_sizes.data(), m_data_sizes.size()*sizeof(m_data_sizes[0])};
        thallium::bulk data_bulk;
        try {
            exposeMetadata();
            data_bulk = exposeData(m_data_segments);
        } catch(const std::exception& ex) {
            setPromises(
                diaspora::Exception{fmt::format(
                    "Unexpected error when registering batch for RDMA: {}", ex.what())});
            return;
        }
        try {
            auto self_addr = static_cast<std::string>(m_engine.self());
            Result<diaspora::EventID> result = m_send_batch_rpc.on(m_partition_ph)(
                m_producer_name, count(),
                BulkRef{m_meta_bulk, 0, m_meta_bulk.size(), self_addr},
                BulkRef{data_bulk, 0, data_bulk.size(), self_addr});
            if(result.success()) {
                setPromises(result.value());
            } else {
                setPromises(diaspora::Exception{result.error()});
            }
        } catch(const std::exception& ex) {
            setPromises(
                diaspora::Exception{fmt::format(
                    "Unexpected error when sending batch: {}", ex.what())});
        }

        m_entries.clear();
        m_meta_sizes.clear();
        m_meta_buffer.clear();
        m_data_sizes.clear();
        m_data_segments.clear();
    }

    size_t count() const override {
        return m_entries.size();
    }

    private:

    void setPromises(diaspora::EventID firstID) {
        auto id = firstID;
        for(auto& entry : m_entries) {
            entry.promise.setValue(id);
            ++id;
        }
    }

    void setPromises(diaspora::Exception ex) {
        for(auto& entry : m_entries) {
            entry.promise.setException(std::move(ex));
        }
    }

    void exposeMetadata() {
        /* first check if we actually need to change the existing bulk */
        auto seg_count = m_meta_bulk.segment_count();
        if(seg_count == 2) {
            auto bulk = m_meta_bulk.get_bulk();
            void* ptrs[2];
            hg_size_t sizes[2];
            hg_uint32_t actual_count;
            hg_return_t ret = margo_bulk_access(
                bulk, 0, m_meta_bulk.size(), HG_BULK_READ_ONLY, 2,
                ptrs, sizes, &actual_count);
            if(ret == HG_SUCCESS
            && ptrs[0] == (void*)m_meta_sizes.data()
            && sizes[0] == m_meta_sizes.size()*sizeof(m_meta_sizes[0])
            && ptrs[1] == (void*)m_meta_buffer.data()
            && sizes[1] == m_meta_buffer.size()*sizeof(m_meta_buffer[0])) {
                return;
            }
        }
        if(m_meta_buffer.size() == 0) {
           m_meta_bulk = thallium::bulk{};
           return;
        }
        std::vector<std::pair<void *, size_t>> segments;
        segments.reserve(2);
        segments.emplace_back(
            const_cast<size_t*>(m_meta_sizes.data()),
            m_meta_sizes.size()*sizeof(m_meta_sizes[0]));
        segments.emplace_back(
            const_cast<char*>(m_meta_buffer.data()),
            m_meta_buffer.size()*sizeof(m_meta_buffer[0]));
        m_meta_bulk = m_engine.expose(segments, thallium::bulk_mode::read_only);
    }

    thallium::bulk exposeData(const std::vector<std::pair<void *, size_t>>& segments) {
        return m_engine.expose(segments, thallium::bulk_mode::read_only);
    }
};

}

#endif
