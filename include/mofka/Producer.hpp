/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_HPP
#define MOFKA_PRODUCER_HPP

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/AsyncRequest.hpp>
#include <memory>

namespace mofka {

class TopicHandle;
class ProducerImpl;

class ProducerOptions {

};

/**
 * @brief A Producer is an object that can emmit events into a its topic.
 */
class Producer {

    friend class TopicHandle;

    public:

    /**
     * @brief Constructor. The resulting Producer handle will be invalid.
     */
    Producer();

    /**
     * @brief Copy-constructor.
     */
    Producer(const Producer&);

    /**
     * @brief Move-constructor.
     */
    Producer(Producer&&);

    /**
     * @brief Copy-assignment operator.
     */
    Producer& operator=(const Producer&);

    /**
     * @brief Move-assignment operator.
     */
    Producer& operator=(Producer&&);

    /**
     * @brief Destructor.
     */
    ~Producer();

    /**
     * @brief Returns the name of the producer.
     */
    const std::string& name() const;

    /**
     * @brief Returns a copy of the options provided when
     * the Producer was created.
     */
    ProducerOptions options() const;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Checks if the Producer instance is valid.
     */
    operator bool() const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a Producer instance.
     *
     * @param impl Pointer to implementation.
     */
    Producer(const std::shared_ptr<ProducerImpl>& impl);

    std::shared_ptr<ProducerImpl> self;
};

}

#endif
