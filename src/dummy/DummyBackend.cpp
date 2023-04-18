/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "DummyBackend.hpp"
#include <iostream>

ALPHA_REGISTER_BACKEND(dummy, DummyResource);

void DummyResource::sayHello() {
    std::cout << "Hello World" << std::endl;
}

alpha::RequestResult<int32_t> DummyResource::computeSum(int32_t x, int32_t y) {
    alpha::RequestResult<int32_t> result;
    result.value() = x + y;
    return result;
}

alpha::RequestResult<bool> DummyResource::destroy() {
    alpha::RequestResult<bool> result;
    result.value() = true;
    // or result.success() = true
    return result;
}

std::unique_ptr<alpha::Backend> DummyResource::create(const thallium::engine& engine, const json& config) {
    (void)engine;
    return std::unique_ptr<alpha::Backend>(new DummyResource(config));
}

std::unique_ptr<alpha::Backend> DummyResource::open(const thallium::engine& engine, const json& config) {
    (void)engine;
    return std::unique_ptr<alpha::Backend>(new DummyResource(config));
}
