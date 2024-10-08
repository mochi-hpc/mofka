/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HPP
#define MOFKA_CONSUMER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
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

#include <thallium.hpp>
#include <memory>

namespace mofka {

/**
 * @brief Interface for Consumer class.
 */
class ConsumerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ConsumerInterface() = default;

    /**
     * @brief Returns the name of the producer.
     */
    virtual const std::string& name() const = 0;

    /**
     * @brief Returns a copy of the options provided when
     * the Consumer was created.
     */
    virtual BatchSize batchSize() const = 0;

    /**
     * @brief Returns the ThreadPool associated with the Consumer.
     */
    virtual ThreadPool threadPool() const = 0;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    virtual TopicHandle topic() const = 0;

    /**
     * @brief Returns the DataBroker used by the Consumer.
     */
    virtual DataBroker dataBroker() const = 0;

    /**
     * @brief Returns the DataSelector used by the Consumer.
     */
    virtual DataSelector dataSelector() const = 0;

    /**
     * @brief Unsubscribe from the topic.
     *
     * This function is not supposed to be called by users directly.
     * It is used by the Consumer wrapping the ConsumerInterface in
     * its destructor to stop events from coming in before destroying
     * the object itself.
     */
    virtual void unsubscribe() = 0;

    /**
     * @brief Pull an Event. This function will immediately
     * return a Future<Event>. Calling wait() on the event will
     * block until an Event is actually available.
     */
    virtual Future<Event> pull() = 0;
};

/**
 * @brief A Consumer is an object that can emmit events into a its topic.
 */
class Consumer {

    public:

    /**
     * @brief Constructor. The resulting Consumer handle will be invalid.
     */
    Consumer(const std::shared_ptr<ConsumerInterface>& impl = nullptr)
    : self{impl} {}

    /**
     * @brief Copy-constructor.
     */
    Consumer(const Consumer&) = default;

    /**
     * @brief Move-constructor.
     */
    Consumer(Consumer&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Consumer& operator=(const Consumer&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Consumer& operator=(Consumer&&) = default;

    /**
     * @brief Destructor.
     */
    ~Consumer();

    /**
     * @brief Returns the name of the producer.
     */
    const std::string& name() const {
        return self->name();
    }

    /**
     * @brief Returns a copy of the options provided when
     * the Consumer was created.
     */
    BatchSize batchSize() const {
        return self->batchSize();
    }

    /**
     * @brief Returns the ThreadPool associated with the Consumer.
     */
    ThreadPool threadPool() const {
        return self->threadPool();
    }

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Returns the DataBroker used by the Consumer.
     */
    DataBroker dataBroker() const {
        return self->dataBroker();
    }

    /**
     * @brief Returns the DataSelector used by the Consumer.
     */
    DataSelector dataSelector() const {
        return self->dataSelector();
    }

    /**
     * @brief Pull an Event. This function will immediately
     * return a Future<Event>. Calling wait() on the event will
     * block until an Event is actually available.
     */
    Future<Event> pull() const {
        return self->pull();
    }

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
    void operator|(EventProcessor processor) const && {
        process(processor, threadPool(), NumEvents::Infinity());
    }

    /**
     * @brief Checks if the Consumer instance is valid.
     */
    operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    std::shared_ptr<ConsumerInterface> self;
};

}

#endif
