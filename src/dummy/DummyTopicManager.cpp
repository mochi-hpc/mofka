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

void DummyTopicManager::sayHello() {
    std::cout << "Hello World" << std::endl;
}

RequestResult<int32_t> DummyTopicManager::computeSum(int32_t x, int32_t y) {
    RequestResult<int32_t> result;
    result.value() = x + y;
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
        const Metadata& serializer) {
    (void)engine;
    return std::unique_ptr<mofka::TopicManager>(new DummyTopicManager(config, validator, serializer));
}

}
