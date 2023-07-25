/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "DummyTopicManager.hpp"
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
        size_t remote_bulk_size,
        size_t data_offset,
        thallium::bulk remote_bulk) {
    RequestResult<EventID> result;

    std::vector<char> buffer(remote_bulk_size);
    auto local_bulk = m_engine.expose(
        {{buffer.data(), buffer.size()}},
        thallium::bulk_mode::write_only);

    local_bulk << remote_bulk.on(sender);

    size_t* metadata_sizes = reinterpret_cast<size_t*>(buffer.data());
    char*   metadata       = buffer.data() + num_events*sizeof(size_t);
    size_t* data_sizes     = reinterpret_cast<size_t*>(buffer.data()+data_offset);
    char*   data           = buffer.data() + +data_offset + num_events*sizeof(size_t);

    auto first_id = m_events.size();
    for(size_t i=0; i < num_events; i++) {
        m_events.emplace_back(metadata, metadata_sizes[i], data, data_sizes[i]);
        metadata += metadata_sizes[i];
        data += data_sizes[i];
    }

    result.value() = first_id;
    return result;
}

RequestResult<bool> DummyTopicManager::destroy() {
    RequestResult<bool> result;
    result.value() = true;
    // or result.success() = true
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
