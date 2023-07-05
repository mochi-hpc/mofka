/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_HPP
#define MOFKA_TOPIC_HANDLE_HPP

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/AsyncRequest.hpp>
#include <mofka/Producer.hpp>
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
     * @brief Checks if the TopicHandle instance is valid.
     */
    operator bool() const;

    /**
     * @brief Creates a Producer object with the specified name
     * and options. If specified, the name will be added to each
     * event's metadata.
     *
     * @param name Name of the producer.
     * @param options Options.
     *
     * @return a Producer object.
     */
    Producer producer(std::string_view name = "",
                      ProducerOptions options = ProducerOptions{}) const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a TopicHandle instance.
     *
     * @param impl Pointer to implementation.
     */
    TopicHandle(const std::shared_ptr<TopicHandleImpl>& impl);

    std::shared_ptr<TopicHandleImpl> self;
};

}

#endif
