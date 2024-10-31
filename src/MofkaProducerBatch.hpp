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

    std::string                m_producer_name;
    thallium::engine           m_engine;
    Serializer                 m_serializer;
    thallium::provider_handle  m_partition_ph;
    thallium::remote_procedure m_send_batch_rpc;

    std::vector<size_t>        m_meta_sizes;      /* size of each serialized metadata object */
    std::vector<char>          m_meta_buffer;     /* packed serialized metadata objects */
    std::vector<size_t>        m_data_sizes;      /* size of the data associated with each metadata */
    std::vector<Data::Segment> m_data_segments;   /* list of data segments */
    size_t                     m_total_data_size = 0; /* sum of sizes in the m_data_segments */

    std::vector<Promise<EventID>> m_promises; /* promise associated with each event */

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
        size_t meta_buffer_size = m_meta_buffer.size();
        BufferWrapperOutputArchive archive(m_meta_buffer);
        m_serializer.serialize(archive, metadata);
        size_t meta_size = m_meta_buffer.size() - meta_buffer_size;
        m_meta_sizes.push_back(meta_size);
        size_t data_size = 0;
        for(const auto& seg : data.segments()) {
            m_data_segments.push_back(seg);
            data_size += seg.size;
        }
        m_data_sizes.push_back(data_size);
        m_total_data_size += data_size;
        m_promises.push_back(std::move(promise));
    }

    void send() override {
        thallium::bulk metadata_content, data_content;
        try {
            metadata_content = exposeMetadata();
            data_content = exposeData();
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
                BulkRef{metadata_content, 0, metadataBulkSize(), self_addr},
                BulkRef{data_content, 0, dataBulkSize(), self_addr});
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
        return m_meta_sizes.size();
    }

    private:

    void setPromises(EventID firstID) {
        auto id = firstID;
        for(auto& promise : m_promises) {
            promise.setValue(id);
            ++id;
        }
    }

    void setPromises(Exception ex) {
        for(auto& promise : m_promises) {
            promise.setException(std::move(ex));
        }
    }

    thallium::bulk exposeMetadata() {
        if(count() == 0) return thallium::bulk{};
        std::vector<std::pair<void *, size_t>> segments;
        segments.reserve(2);
        segments.emplace_back(m_meta_sizes.data(), m_meta_sizes.size()*sizeof(m_meta_sizes[0]));
        segments.emplace_back(m_meta_buffer.data(), m_meta_buffer.size()*sizeof(m_meta_buffer[0]));
        return m_engine.expose(segments, thallium::bulk_mode::read_only);
    }

    thallium::bulk exposeData() {
        if(count() == 0) return thallium::bulk{};
        std::vector<std::pair<void *, size_t>> segments;
        segments.reserve(1 + m_data_segments.size());
        segments.emplace_back(m_data_sizes.data(), m_data_sizes.size()*sizeof(m_data_sizes[0]));
        for(const auto& seg : m_data_segments) {
            if(!seg.size) continue;
            segments.emplace_back(const_cast<void*>(seg.ptr), seg.size);
        }
        return m_engine.expose(segments, thallium::bulk_mode::read_only);
    }

    size_t metadataBulkSize() const {
        return count()*sizeof(size_t) + m_meta_buffer.size();
    }

    size_t dataBulkSize() const {
        return count()*sizeof(size_t) + m_total_data_size;
    }

};

}

#endif
