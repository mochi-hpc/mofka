/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BACKEND_HPP
#define MOFKA_BACKEND_HPP

#include <mofka/RequestResult.hpp>
#include <mofka/Json.hpp>
#include <thallium.hpp>
#include <unordered_map>
#include <string_view>
#include <functional>

/**
 * @brief Helper class to register backend types into the backend factory.
 */
template<typename TopicManagerType>
class __MofkaTopicManagerRegistration;

namespace mofka {

/**
 * @brief Interface for topic backends. To build a new backend,
 * implement a class MyTopicManager that inherits from TopicManager, and put
 * MOFKA_REGISTER_BACKEND(mybackend, MyTopicManager); in a cpp file
 * that includes your backend class' header file.
 */
class TopicManager {

    public:

    /**
     * @brief Constructor.
     */
    TopicManager() = default;

    /**
     * @brief Move-constructor.
     */
    TopicManager(TopicManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    TopicManager(const TopicManager&) = default;

    /**
     * @brief Move-assignment operator.
     */
    TopicManager& operator=(TopicManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    TopicManager& operator=(const TopicManager&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~TopicManager() = default;

    /**
     * @brief Prints Hello World.
     */
    virtual void sayHello() = 0;

    /**
     * @brief Compute the sum of two integers.
     *
     * @param x first integer
     * @param y second integer
     *
     * @return a RequestResult containing the result.
     */
    virtual RequestResult<int32_t> computeSum(int32_t x, int32_t y) = 0;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual RequestResult<bool> destroy() = 0;

};

/**
 * @brief The TopicFactory contains functions to create Topic objects.
 */
class TopicFactory {

    template<typename TopicManagerType>
    friend class ::__MofkaTopicManagerRegistration;

    public:

    TopicFactory() = delete;

    /**
     * @brief Creates a topic and returns a unique_ptr to the created instance.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration object to pass to the backend's create function.
     *
     * @return a unique_ptr to the created Topic.
     */
    static std::unique_ptr<TopicManager> createTopic(
            std::string_view backend_name,
            const thallium::engine& engine,
            const rapidjson::Value& config);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<TopicManager>(const thallium::engine&, const rapidjson::Value&)>> create_fn;
};

} // namespace mofka


#define MOFKA_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __MofkaTopicManagerRegistration<__backend_type> __mofka ## __backend_name ## _backend( #__backend_name )

template<typename TopicManagerType>
class __MofkaTopicManagerRegistration {

    public:

    __MofkaTopicManagerRegistration(std::string_view backend_name)
    {
        mofka::TopicFactory::create_fn.emplace(
            backend_name, [](const thallium::engine& engine, const rapidjson::Value& config) {
                return TopicManagerType::create(engine, config);
            });
    }
};

#endif
