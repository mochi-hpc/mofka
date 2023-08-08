/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "DummyTopicManager.hpp"
#include "mofka/DataDescriptor.hpp"
#include "mofka/BufferWrapperArchive.hpp"
#include <numeric>
#include <iostream>

MOFKA_REGISTER_BACKEND(dummy, mofka::DummyTopicManager);

namespace mofka {

Metadata DummyTopicManager::getValidatorMetadata() const {
    return m_validator;
}

Metadata DummyTopicManager::getSerializerMetadata() const {
    return m_serializer;
}

Metadata DummyTopicManager::getTargetSelectorMetadata() const {
    return m_selector;
}

RequestResult<EventID> DummyTopicManager::receiveBatch(
          const thallium::endpoint& sender,
          const std::string& producer_name,
          size_t num_events,
          const BulkRef& metadata_bulk,
          const BulkRef& data_bulk)
{
    (void)producer_name;
    RequestResult<EventID> result;
    EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_events_mtx};
        first_id = m_events_metadata_sizes.size();
        // --------- transfer the metadata
        auto metadata_size = metadata_bulk.size - num_events*sizeof(size_t);
        // resize the sizes and offsets arrays, and metadata array
        if(m_events_metadata_sizes.capacity() < first_id + num_events) {
            m_events_metadata_sizes.reserve(2*(first_id + num_events));
        }
        if(m_events_metadata_offsets.capacity() < first_id + num_events) {
            m_events_metadata_offsets.reserve(2*(first_id + num_events));
        }
        m_events_metadata_sizes.resize(first_id + num_events);
        m_events_metadata_offsets.resize(first_id + num_events);
        size_t first_metadata_offset = m_events_metadata.size();
        if(m_events_metadata.capacity() < first_metadata_offset + metadata_size) {
            m_events_metadata.reserve(2*(first_metadata_offset + metadata_size));
        }
        m_events_metadata.resize(first_metadata_offset + metadata_size);
        // transfer the metadata sizes and content
        auto local_metadata_bulk = m_engine.expose(
            {{(char*)(m_events_metadata_sizes.data() + first_id), num_events*sizeof(size_t)},
             {m_events_metadata.data() + first_metadata_offset, metadata_size}},
            thallium::bulk_mode::write_only);
        local_metadata_bulk << metadata_bulk.handle.on(sender).select(
            metadata_bulk.offset, metadata_bulk.size);
        // TODO check that metadata_size = sum of m_events_metadata_sizes recveived
        // update the metadata offsets vector
        auto metadata_offset = first_metadata_offset;
        for(size_t i = first_id; i < m_events_metadata_sizes.size(); ++i) {
            m_events_metadata_offsets[i] = metadata_offset;
            metadata_offset += m_events_metadata_sizes[i];
        }
        // --------- transfer the data
        auto data_size = data_bulk.size - num_events*sizeof(size_t);
        // resize the sizes and offsets arrays, and data array
        if(m_events_data_sizes.capacity() < first_id + num_events) {
            m_events_data_sizes.reserve(2*(first_id + num_events));
        }
        if(m_events_data_offsets.capacity() < first_id + num_events) {
            m_events_data_offsets.reserve(2*(first_id + num_events));
        }
        m_events_data_sizes.resize(first_id + num_events);
        m_events_data_offsets.resize(first_id + num_events);
        size_t first_data_offset = m_events_data.size();
        if(m_events_data.capacity() < first_data_offset + data_size) {
            m_events_data.reserve(2*(first_data_offset + data_size));
        }
        m_events_data.resize(first_data_offset + data_size);
        // transfer the data sizes and content
        auto local_data_bulk = m_engine.expose(
            {{(char*)(m_events_data_sizes.data() + first_id), num_events*sizeof(size_t)},
             {m_events_data.data() + first_data_offset, data_size}},
            thallium::bulk_mode::write_only);
        local_data_bulk << data_bulk.handle.on(sender).select(
            data_bulk.offset, data_bulk.size);
        // TODO check that data_size = sum of m_events_data_sizes recveived
        // update the data offsets vector
        auto data_offset = first_data_offset;
        for(size_t i = first_id; i < m_events_data_sizes.size(); ++i) {
            m_events_data_offsets[i] = data_offset;
            data_offset += m_events_data_sizes[i];
        }
        // update the DataDescriptor information
        size_t data_desc_offset = 0;
        if(!m_events_data_desc_offsets.empty())
            data_desc_offset = m_events_data_desc_offsets.back() + m_events_data_desc_sizes.back();
        m_events_data_desc_sizes.resize(first_id + num_events);
        m_events_data_desc_offsets.resize(first_id + num_events);
        BufferWrapperOutputArchive output_archive{m_events_data_desc};
        for(size_t i = first_id; i < m_events_data_desc_sizes.size(); ++i) {
            auto offset_size = OffsetSize{m_events_data_offsets[i], m_events_data_sizes[i]};
            auto data_descriptor = DataDescriptor::From(offset_size.toString(), offset_size.size);
            size_t m_events_data_desc_size = m_events_data_desc.size();
            data_descriptor.save(output_archive);
            auto data_descriptor_size = m_events_data_desc.size() - m_events_data_desc_size;
            m_events_data_desc_sizes[i] = data_descriptor_size;
            m_events_data_desc_offsets[i] = data_desc_offset;
            data_desc_offset += data_descriptor_size;
        }
    }
    m_events_cv.notify_all();
    result.value() = first_id;
    return result;
}

void DummyTopicManager::wakeUp() {
    m_events_cv.notify_all();
}

RequestResult<void> DummyTopicManager::feedConsumer(
    ConsumerHandle consumerHandle,
    BatchSize batchSize) {
    RequestResult<void> result;

    if(batchSize.value == 0)
        batchSize = BatchSize::Adaptive();
    EventID first_id;
    {
        auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
        first_id = m_consumer_cursor[consumerHandle.name()];
    }

    auto self_addr = static_cast<std::string>(m_engine.self());
    {
        auto g = std::unique_lock<thallium::mutex>{m_events_mtx};
        while(!consumerHandle.shouldStop()) {
            size_t num_events_to_send;
            bool should_stop = false;
            while(true) {
                // find the number of events we can send
                size_t max_available_events = m_events_metadata_sizes.size() - first_id;
                num_events_to_send = std::min(batchSize.value, max_available_events);
                should_stop = consumerHandle.shouldStop();
                if(num_events_to_send != 0 || should_stop) break;
                m_events_cv.wait(g);
            }
            if(should_stop) break;

            // find the range of metadata sizes
            const auto metadata_sizes_ptr = m_events_metadata_sizes.data() + first_id;
            // find the metadata content
            const auto metadata_ptr = m_events_metadata.data() + m_events_metadata_offsets[first_id];
            const auto metadata_size = std::accumulate(
                    metadata_sizes_ptr, metadata_sizes_ptr + num_events_to_send, (size_t)0);
            // create the BulkRefs for the metadata sizes and contents
            auto metadata_bulk = m_engine.expose(
                    {{metadata_sizes_ptr, num_events_to_send*sizeof(size_t)},
                     {metadata_ptr, metadata_size}},
                    thallium::bulk_mode::read_only);
            auto metadata_size_bulk_ref = BulkRef{
                metadata_bulk, 0, num_events_to_send*sizeof(size_t), self_addr
            };
            auto metadata_bulk_ref = BulkRef{
                metadata_bulk, num_events_to_send*sizeof(size_t), metadata_size, self_addr
            };

            // find the range of descriptor sizes
            const auto descriptors_sizes_ptr = m_events_data_desc_sizes.data() + first_id;
            // find the descriptors content
            const auto descriptors_ptr = m_events_data_desc.data() + m_events_data_desc_offsets[first_id];
            const auto descriptors_size = std::accumulate(
                    descriptors_sizes_ptr, descriptors_sizes_ptr + num_events_to_send, (size_t)0);
            // create BulRefs for the descriptor sizes and contents
            auto data_descriptors_bulk = m_engine.expose(
                    {{descriptors_sizes_ptr, num_events_to_send*sizeof(size_t)},
                     {descriptors_ptr, descriptors_size}},
                    thallium::bulk_mode::read_only);
            // create the BulkRefs for the data descriptors
            auto data_desc_size_bulk_ref = BulkRef{
                data_descriptors_bulk, 0, num_events_to_send*sizeof(size_t), self_addr
            };
            auto data_desc_bulk_ref = BulkRef{
                data_descriptors_bulk, num_events_to_send*sizeof(size_t), descriptors_size, self_addr
            };
            // feed consumer
            consumerHandle.feed(
                    num_events_to_send,
                    first_id,
                    metadata_size_bulk_ref,
                    metadata_bulk_ref,
                    data_desc_size_bulk_ref,
                    data_desc_bulk_ref);

            first_id += num_events_to_send;
        }
    }

    return result;
}

RequestResult<void> DummyTopicManager::acknowledge(
    std::string_view consumer_name,
    EventID event_id) {
    RequestResult<void> result;
    auto g = std::unique_lock<thallium::mutex>{m_consumer_cursor_mtx};
    std::string consumer_name_str{consumer_name.data(), consumer_name.size()};
    m_consumer_cursor[consumer_name_str] = event_id + 1;
    return result;
}

RequestResult<bool> DummyTopicManager::destroy() {
    RequestResult<bool> result;
    // TODO wait for all the consumers to be done consuming
    result.value() = true;
    return result;
}

std::unique_ptr<mofka::TopicManager> DummyTopicManager::create(
        const thallium::engine& engine,
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer) {
    return std::unique_ptr<mofka::TopicManager>(
        new DummyTopicManager(config, validator, selector, serializer, engine));
}

}
