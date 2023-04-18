/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "DummyTopicManager.hpp"
#include <iostream>

MOFKA_REGISTER_BACKEND(dummy, DummyTopicManager);

void DummyTopicManager::sayHello() {
    std::cout << "Hello World" << std::endl;
}

mofka::RequestResult<int32_t> DummyTopicManager::computeSum(int32_t x, int32_t y) {
    mofka::RequestResult<int32_t> result;
    result.value() = x + y;
    return result;
}

mofka::RequestResult<bool> DummyTopicManager::destroy() {
    mofka::RequestResult<bool> result;
    result.value() = true;
    // or result.success() = true
    return result;
}

std::unique_ptr<mofka::TopicManager> DummyTopicManager::create(
        const thallium::engine& engine, const rapidjson::Value& config) {
    (void)engine;
    return std::unique_ptr<mofka::TopicManager>(new DummyTopicManager(config));
}
