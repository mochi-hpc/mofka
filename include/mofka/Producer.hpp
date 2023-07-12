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
#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <mofka/EventID.hpp>
#include <mofka/Future.hpp>
#include <mofka/ThreadPool.hpp>
#include <memory>

namespace mofka {

class TopicHandle;
class ActiveBatchQueue;
class ProducerImpl;

/**
 * @brief Strongly typped size_t meant to store the batch size to
 * use when creating a Producer.
 */
struct BatchSize {

    std::size_t value;

    explicit constexpr BatchSize(std::size_t val)
    : value(val) {}

    /**
     * @brief Returns a value telling the producer to try its best
     * to adapt the batch size to the use-case and workload.
     */
    static BatchSize Adaptive();

    inline bool operator<(const BatchSize& other) const { return value < other.value; }
    inline bool operator>(const BatchSize& other) const { return value > other.value; }
    inline bool operator<=(const BatchSize& other) const { return value <= other.value; }
    inline bool operator>=(const BatchSize& other) const { return value >= other.value; }
    inline bool operator==(const BatchSize& other) const { return value == other.value; }
    inline bool operator!=(const BatchSize& other) const { return value != other.value; }
};

/**
 * @brief A Producer is an object that can emmit events into a its topic.
 */
class Producer {

    friend class TopicHandle;
    friend class ActiveBatchQueue;

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
    BatchSize batchSize() const;

    /**
     * @brief Returns the ThreadPool associated with the Producer.
     */
    ThreadPool threadPool() const;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Checks if the Producer instance is valid.
     */
    operator bool() const;

    /**
     * @brief Pushes an event into the producer's underlying topic,
     * returning a Future that can be awaited.
     *
     * @param metadata Metadata of the event.
     * @param data Optional data to attach to the event.
     *
     * @return a Future<EventID> tracking the asynchronous operation.
     */
    Future<EventID> push(Metadata metadata, Data data = Data{}) const;

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
