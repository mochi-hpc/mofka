/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __DUMMY_BACKEND_HPP
#define __DUMMY_BACKEND_HPP

#include <mofka/Backend.hpp>

using json = nlohmann::json;

/**
 * Dummy implementation of an mofka Backend.
 */
class DummyTopic : public mofka::Backend {
   
    json m_config;

    public:

    /**
     * @brief Constructor.
     */
    DummyTopic(const json& config)
    : m_config(config) {}

    /**
     * @brief Move-constructor.
     */
    DummyTopic(DummyTopic&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DummyTopic(const DummyTopic&) = default;

    /**
     * @brief Move-assignment operator.
     */
    DummyTopic& operator=(DummyTopic&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DummyTopic& operator=(const DummyTopic&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~DummyTopic() = default;

    /**
     * @brief Prints Hello World.
     */
    void sayHello() override;

    /**
     * @brief Compute the sum of two integers.
     *
     * @param x first integer
     * @param y second integer
     *
     * @return a RequestResult containing the result.
     */
    mofka::RequestResult<int32_t> computeSum(int32_t x, int32_t y) override;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    mofka::RequestResult<bool> destroy() override;

    /**
     * @brief Static factory function used by the TopicFactory to
     * create a DummyTopic.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the topic
     *
     * @return a unique_ptr to a topic
     */
    static std::unique_ptr<mofka::Backend> create(const thallium::engine& engine, const json& config);

    /**
     * @brief Static factory function used by the TopicFactory to
     * open a DummyTopic.
     *
     * @param engine Thallium engine
     * @param config JSON configuration for the topic
     *
     * @return a unique_ptr to a topic
     */
    static std::unique_ptr<mofka::Backend> open(const thallium::engine& engine, const json& config);
};

#endif
