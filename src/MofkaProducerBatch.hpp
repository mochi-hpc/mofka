/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_BATCH_IMPL_H
#define MOFKA_PRODUCER_BATCH_IMPL_H

#include "MofkaPartitionInfo.hpp"
#include "MofkaProducer.hpp"
#include "Promise.hpp"
#include "DataImpl.hpp"
#include "PimplUtil.hpp"
#include "ProducerBatchInterface.hpp"

#include "mofka/BulkRef.hpp"
#include "mofka/Result.hpp"
#include "mofka/EventID.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Data.hpp"
#include "mofka/Future.hpp"
#include "mofka/Producer.hpp"
#include "mofka/BufferWrapperArchive.hpp"

#include <thallium.hpp>
#include <mutex>
#include <queue>
#include <vector>
#include <cstdint>

namespace mofka {

namespace tl = thallium;

class MofkaProducerBatch : public ProducerBatchInterface {

    struct Entry {
        Metadata         metadata;
        Data             data;
        Promise<EventID> promise;
    };

    std::string                m_producer_name;
    thallium::engine           m_engine;
    Serializer                 m_serializer;
    thallium::provider_handle  m_partition_ph;
    thallium::remote_procedure m_send_batch_rpc;

    std::vector<Entry> m_entries;

    public:

    MofkaProducerBatch(
        std::string producer_name,
        thallium::engine engine,
        Serializer serializer,
        thallium::provider_handle partition_ph,
        thallium::remote_procedure send_batch)
    : m_producer_name{std::move(producer_name)}
    , m_engine{std::move(engine)}
    , m_serializer{std::move(serializer)}
    , m_partition_ph{std::move(partition_ph)}
    , m_send_batch_rpc{std::move(send_batch)}
    {}

    void push(
            const Metadata& metadata,
            const Data& data,
            Promise<EventID> promise) override {
        m_entries.push_back({metadata, data, std::move(promise)});
    }

    void send() override {

        std::vector<size_t>                   meta_sizes;
        std::vector<char>                     meta_buffer;
        std::vector<size_t>                   data_sizes;
        std::vector<std::pair<void*, size_t>> data_segments(1);

        meta_sizes.reserve(m_entries.size());
        bool first_entry = true;
        BufferWrapperOutputArchive archive(meta_buffer);
        for(auto& entry : m_entries) {
            size_t meta_buffer_size = meta_buffer.size();
            m_serializer.serialize(archive, entry.metadata);
            size_t meta_size = meta_buffer.size() - meta_buffer_size;
            if(first_entry) {
                // use the first entry metadata size as an estimate for the total size
                size_t estimated_total_size = meta_size * m_entries.size() * 1.1;
                meta_buffer.reserve(estimated_total_size);
                first_entry = false;
            }
            meta_sizes.push_back(meta_size);
            size_t data_size = 0;
            for(const auto& seg : entry.data.segments()) {
                if(seg.size == 0) continue;
                data_segments.emplace_back(seg.ptr, seg.size);
                data_size += seg.size;
            }
            data_sizes.push_back(data_size);
        }
        data_segments[0] = {data_sizes.data(), data_sizes.size()*sizeof(data_sizes[0])};
        thallium::bulk metadata_content, data_content;
        try {
            metadata_content = exposeMetadata(meta_buffer, meta_sizes);
            data_content = exposeData(data_segments);
        } catch(const std::exception& ex) {
            setPromises(
                Exception{fmt::format(
                    "Unexpected error when registering batch for RDMA: {}", ex.what())});
            return;
        }
        try {
            auto self_addr = static_cast<std::string>(m_engine.self());
            Result<EventID> result = m_send_batch_rpc.on(m_partition_ph)(
                m_producer_name, count(),
                BulkRef{metadata_content, 0, metadata_content.size(), self_addr},
                BulkRef{data_content, 0, data_content.size(), self_addr});
            if(result.success()) {
                setPromises(result.value());
            } else {
                setPromises(Exception{result.error()});
            }
        } catch(const std::exception& ex) {
            setPromises(
                Exception{fmt::format(
                    "Unexpected error when sending batch: {}", ex.what())});
        }
    }

    size_t count() const override {
        return m_entries.size();
    }

    private:

    void setPromises(EventID firstID) {
        auto id = firstID;
        for(auto& entry : m_entries) {
            entry.promise.setValue(id);
            ++id;
        }
    }

    void setPromises(Exception ex) {
        for(auto& entry : m_entries) {
            entry.promise.setException(std::move(ex));
        }
    }

    thallium::bulk exposeMetadata(
            const std::vector<char>& meta_buffer,
            const std::vector<size_t>& meta_sizes) {
        if(meta_buffer.size() == 0) return thallium::bulk{};
        std::vector<std::pair<void *, size_t>> segments;
        segments.reserve(2);
        segments.emplace_back(
            const_cast<size_t*>(meta_sizes.data()),
            meta_sizes.size()*sizeof(meta_sizes[0]));
        segments.emplace_back(
            const_cast<char*>(meta_buffer.data()),
            meta_buffer.size()*sizeof(meta_buffer[0]));
        return m_engine.expose(segments, thallium::bulk_mode::read_only);
    }

    thallium::bulk exposeData(const std::vector<std::pair<void *, size_t>>& segments) {
        return m_engine.expose(segments, thallium::bulk_mode::read_only);
    }
};

}

#endif
