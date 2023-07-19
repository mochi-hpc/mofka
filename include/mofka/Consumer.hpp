/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HPP
#define MOFKA_CONSUMER_HPP

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/AsyncRequest.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <mofka/EventID.hpp>
#include <mofka/Future.hpp>
#include <mofka/ThreadPool.hpp>
#include <mofka/DataBroker.hpp>
#include <mofka/DataSelector.hpp>
#include <mofka/EventProcessor.hpp>
#include <mofka/BatchSize.hpp>
#include <mofka/NumEvents.hpp>
#include <memory>

namespace mofka {

class TopicHandle;
class ActiveBatchQueue;
class ConsumerImpl;

/**
 * @brief A Consumer is an object that can emmit events into a its topic.
 */
class Consumer {

    friend class TopicHandle;
    friend class ActiveBatchQueue;

    public:

    /**
     * @brief Constructor. The resulting Consumer handle will be invalid.
     */
    Consumer();

    /**
     * @brief Copy-constructor.
     */
    Consumer(const Consumer&);

    /**
     * @brief Move-constructor.
     */
    Consumer(Consumer&&);

    /**
     * @brief Copy-assignment operator.
     */
    Consumer& operator=(const Consumer&);

    /**
     * @brief Move-assignment operator.
     */
    Consumer& operator=(Consumer&&);

    /**
     * @brief Destructor.
     */
    ~Consumer();

    /**
     * @brief Returns the name of the producer.
     */
    const std::string& name() const;

    /**
     * @brief Returns a copy of the options provided when
     * the Consumer was created.
     */
    BatchSize batchSize() const;

    /**
     * @brief Returns the ThreadPool associated with the Consumer.
     */
    ThreadPool threadPool() const;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Returns the DataBroker used by the Consumer.
     */
    DataBroker dataBroker() const;

    /**
     * @brief Returns the DataSelector used by the Consumer.
     */
    DataSelector dataSelector() const;

    /**
     * @brief Pull an Event. This function will immediately
     * return a Future<Event>. Calling wait() on the event will
     * block until an Event is actually available.
     */
    Future<Event> pull() const;

    /**
     * @brief Feed the Events pulled by the Consumer into the provided
     * EventProcessor function. The Consumer will stop feeding the processor
     * if it raises a StopEventProcessor exception, or after maxEvents events
     * have been processed.
     *
     * @note Calling process from multiple threads concurrently on the same
     * consumer is not allowed and will throw an exception.
     *
     * @param processor EventProcessor.
     */
    void process(EventProcessor processor,
                 ThreadPool threadPool = ThreadPool{},
                 NumEvents maxEvents = NumEvents::Infinity()) const;

    /**
     * @brief This method is syntactic sugar to call process with
     * the threadPool set to the same ThreadPool as the Consumer
     * and a maxEvents set to infinity.
     *
     * Note: this method can only be called on a rvalue reference to a
     * Consumer, e.g. doing:
     * ```
     * topic.consumer("myconsumer", ...) | processor;
     * ```
     * This is to prevent another thread from calling it with another
     * processor.
     *
     * @param processor EventProcessor.
     */
    void operator|(EventProcessor processor) const &&;

    /**
     * @brief Checks if the Consumer instance is valid.
     */
    operator bool() const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a Consumer instance.
     *
     * @param impl Pointer to implementation.
     */
    Consumer(const std::shared_ptr<ConsumerImpl>& impl);

    std::shared_ptr<ConsumerImpl> self;
};

}

#endif
