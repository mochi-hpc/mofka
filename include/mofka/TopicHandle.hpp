/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_HPP
#define MOFKA_TOPIC_HANDLE_HPP

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <mofka/ArgsUtil.hpp>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/AsyncRequest.hpp>
#include <mofka/Producer.hpp>
#include <mofka/Consumer.hpp>
#include <mofka/DataBroker.hpp>
#include <mofka/DataSelector.hpp>
#include <memory>
#include <unordered_set>

namespace mofka {

class ServiceHandle;
class TopicHandleImpl;

/**
 * @brief A TopicHandle object is a handle for a remote topic
 * on a set of servers. It enables invoking the topic's functionalities.
 */
class TopicHandle {

    friend class ServiceHandle;
    friend class Producer;
    friend class Consumer;

    public:

    /**
     * @brief Constructor. The resulting TopicHandle handle will be invalid.
     */
    TopicHandle();

    /**
     * @brief Copy-constructor.
     */
    TopicHandle(const TopicHandle&);

    /**
     * @brief Move-constructor.
     */
    TopicHandle(TopicHandle&&);

    /**
     * @brief Copy-assignment operator.
     */
    TopicHandle& operator=(const TopicHandle&);

    /**
     * @brief Move-assignment operator.
     */
    TopicHandle& operator=(TopicHandle&&);

    /**
     * @brief Destructor.
     */
    ~TopicHandle();

    /**
     * @brief Returns the name of the topic.
     */
    const std::string& name() const;

    /**
     * @brief Returns the ServiceHandle this topic has been opened with.
     */
    ServiceHandle service() const;

    /**
     * @brief Creates a Producer object with the specified options.
     * This function allows providing the options in any order,
     * and ommit non-mandatory options. See TopicHandle::makeProducer
     * for the documentation on all the options.
     *
     * @return a Producer object.
     */
    template<typename ... Options>
    Producer producer(Options&&... opts) const {
        return makeProducer(
            GetArgOrDefault(std::string_view{""}, std::forward<Options>(opts)...),
            GetArgOrDefault(BatchSize::Adaptive(), std::forward<Options>(opts)...),
            GetArgOrDefault(ThreadPool{}, std::forward<Options>(opts)...));
    }

    /**
     * @brief Creates a Consumer object with the specified options.
     *
     * This function allows providing the options in any order,
     * and ommit non-mandatory options. See TopicHandle::makeConsumer
     * for the documentation on all the options.
     *
     * @return a Consumer object.
     */
    template<typename ... Options>
    Consumer consumer(std::string_view name, Options&&... opts) const {
        return makeConsumer(name,
            GetArgOrDefault(BatchSize::Adaptive(), std::forward<Options>(opts)...),
            GetArgOrDefault(ThreadPool{}, std::forward<Options>(opts)...),
            GetArgOrDefault(DataBroker{}, std::forward<Options>(opts)...),
            GetArgOrDefault(DataSelector{}, std::forward<Options>(opts)...),
            GetArgOrDefault(targets(), std::forward<Options>(opts)...));
    }

    /**
     * @brief Returns the list of PartitionTargetInfo of the underlying topic.
     */
    const std::vector<PartitionTargetInfo>& targets() const;

    /**
     * @brief Checks if the TopicHandle instance is valid.
     */
    operator bool() const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a TopicHandle instance.
     *
     * @param impl Pointer to implementation.
     */
    TopicHandle(const std::shared_ptr<TopicHandleImpl>& impl);

    std::shared_ptr<TopicHandleImpl> self;

    /**
     * @brief Create a Producer object from the full
     * list of optional arguments.
     *
     * @param name Name of the Producer.
     * @param batch_size Batch size.
     * @param thread_pool Thread pool.
     *
     * @return Producer instance.
     */
    Producer makeProducer(std::string_view name,
                          BatchSize batch_size,
                          ThreadPool thread_pool) const;

    /**
     * @brief Create a Consumer object from the full
     * list of optional and mandatory arguments.
     *
     * @param name Name of the Consumer.
     * @param batch_size Batch size.
     * @param thread_pool Thread pool.
     * @param data_broker Data broker.
     * @param data_selector Data selector.
     *
     * @return Consumer instance.
     */
    Consumer makeConsumer(std::string_view name,
                          BatchSize batch_size,
                          ThreadPool thread_pool,
                          DataBroker data_broker,
                          DataSelector data_selector,
                          const std::vector<PartitionTargetInfo>& targets) const;

};

}

#endif
