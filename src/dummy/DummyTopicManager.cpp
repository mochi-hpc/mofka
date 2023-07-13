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
        const std::string& producer_name,
        size_t num_events,
        size_t remote_bulk_size,
        size_t data_offset,
        thallium::bulk remote_bulk) {
    RequestResult<EventID> result;
    result.value() = 0;
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
    (void)engine;
    return std::unique_ptr<mofka::TopicManager>(
        new DummyTopicManager(config, validator, selector, serializer));
}

}
