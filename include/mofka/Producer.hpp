/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_HPP
#define MOFKA_PRODUCER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <mofka/EventID.hpp>
#include <mofka/Future.hpp>
#include <mofka/ThreadPool.hpp>
#include <mofka/BatchSize.hpp>

#include <thallium.hpp>
#include <memory>

namespace mofka {

class ActiveProducerBatchQueue;

/**
 * @brief Interface for Producer.
 */
class ProducerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ProducerInterface() = default;

    /**
     * @brief Returns the name of the producer.
     */
    virtual const std::string& name() const = 0;

    /**
     * @brief Returns a copy of the options provided when
     * the Producer was created.
     */
    virtual BatchSize batchSize() const = 0;

    /**
     * @brief Returns the ThreadPool associated with the Producer.
     */
    virtual ThreadPool threadPool() const = 0;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    virtual TopicHandle topic() const = 0;

    /**
     * @brief Pushes an event into the producer's underlying topic,
     * returning a Future that can be awaited.
     *
     * @param metadata Metadata of the event.
     * @param data Optional data to attach to the event.
     *
     * @return a Future<EventID> tracking the asynchronous operation.
     */
    virtual Future<EventID> push(Metadata metadata, Data data = Data{}) = 0;

    /**
     * @brief Block until all the pending events have been sent.
     */
    virtual void flush() = 0;

};

/**
 * @brief A Producer is an object that can emmit events into a its topic.
 */
class Producer {

    friend class TopicHandle;
    friend class ActiveProducerBatchQueue;

    public:

    /**
     * @brief Constructor.
     */
    Producer(const std::shared_ptr<ProducerInterface>& impl = nullptr)
    : self{impl} {}

    /**
     * @brief Copy-constructor.
     */
    Producer(const Producer&) = default;

    /**
     * @brief Move-constructor.
     */
    Producer(Producer&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Producer& operator=(const Producer&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Producer& operator=(Producer&&) = default;

    /**
     * @brief Destructor.
     */
    ~Producer() = default;

    /**
     * @brief Returns the name of the producer.
     */
    const std::string& name() const {
        return self->name();
    }

    /**
     * @brief Returns a copy of the options provided when
     * the Producer was created.
     */
    BatchSize batchSize() const {
        return self->batchSize();
    }

    /**
     * @brief Returns the ThreadPool associated with the Producer.
     */
    ThreadPool threadPool() const {
        return self->threadPool();
    }

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Checks if the Producer instance is valid.
     */
    operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Pushes an event into the producer's underlying topic,
     * returning a Future that can be awaited.
     *
     * @param metadata Metadata of the event.
     * @param data Optional data to attach to the event.
     *
     * @return a Future<EventID> tracking the asynchronous operation.
     */
    Future<EventID> push(Metadata metadata, Data data = Data{}) const {
        return self->push(metadata, data);
    }

    /**
     * @brief Block until all the pending events have been sent.
     */
    void flush() {
        return self->flush();
    }

    private:

    std::shared_ptr<ProducerInterface> self;
};

}

#endif
