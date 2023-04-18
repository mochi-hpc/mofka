/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "DummyBackend.hpp"
#include <iostream>

MOFKA_REGISTER_BACKEND(dummy, DummyTopic);

void DummyTopic::sayHello() {
    std::cout << "Hello World" << std::endl;
}

mofka::RequestResult<int32_t> DummyTopic::computeSum(int32_t x, int32_t y) {
    mofka::RequestResult<int32_t> result;
    result.value() = x + y;
    return result;
}

mofka::RequestResult<bool> DummyTopic::destroy() {
    mofka::RequestResult<bool> result;
    result.value() = true;
    // or result.success() = true
    return result;
}

std::unique_ptr<mofka::Backend> DummyTopic::create(const thallium::engine& engine, const json& config) {
    (void)engine;
    return std::unique_ptr<mofka::Backend>(new DummyTopic(config));
}

std::unique_ptr<mofka::Backend> DummyTopic::open(const thallium::engine& engine, const json& config) {
    (void)engine;
    return std::unique_ptr<mofka::Backend>(new DummyTopic(config));
}
