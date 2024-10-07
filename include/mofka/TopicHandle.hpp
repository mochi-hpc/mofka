/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_HPP
#define MOFKA_TOPIC_HANDLE_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/ArgsUtil.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Producer.hpp>
#include <mofka/Consumer.hpp>
#include <mofka/DataBroker.hpp>
#include <mofka/DataSelector.hpp>
#include <mofka/Ordering.hpp>

#include <thallium.hpp>
#include <memory>
#include <unordered_set>

namespace mofka {

class TopicHandleInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~TopicHandleInterface() = default;

    /**
     * @brief Returns the name of the topic.
     */
    virtual const std::string& name() const = 0;

    /**
     * @brief Returns the list of PartitionInfo of the underlying topic.
     */
    virtual const std::vector<PartitionInfo>& partitions() const = 0;

    /**
     * @brief Return the Validator of the topic.
     */
    virtual Validator validator() const = 0;

    /**
     * @brief Return the PartitionSelector of the topic.
     */
    virtual PartitionSelector selector() const = 0;

    /**
     * @brief Return the Serializer of the topic.
     */
    virtual Serializer serializer() const = 0;

    /**
     * @brief Indicate to the partition servers that no more events will be produced
     * in this topic. This will make any attempt to consume events return events with
     * no metadata, no data, and an ID of NoMoreEvents.
     */
    virtual void markAsComplete() const = 0;

    /**
     * @brief Create a Producer object from the full
     * list of optional arguments.
     *
     * @param name Name of the Producer.
     * @param batch_size Batch size.
     * @param thread_pool Thread pool.
     * @param ordering Whether to enforce strict ordering.
     *
     * @return Producer instance.
     */
    virtual Producer makeProducer(std::string_view name,
                                  BatchSize batch_size,
                                  ThreadPool thread_pool,
                                  Ordering ordering) const = 0;

    /**
     * @brief Create a Consumer object from the full
     * list of optional and mandatory arguments.
     *
     * @param name Name of the Consumer.
     * @param batch_size Batch size.
     * @param thread_pool Thread pool.
     * @param data_broker Data broker.
     * @param data_selector Data selector.
     * @param targets Indices of the partitions to consumer from.
     *
     * @return Consumer instance.
     */
    virtual Consumer makeConsumer(std::string_view name,
                                  BatchSize batch_size,
                                  ThreadPool thread_pool,
                                  DataBroker data_broker,
                                  DataSelector data_selector,
                                  const std::vector<size_t>& targets) const = 0;

};

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
     * @brief Constructor.
     */
    TopicHandle(const std::shared_ptr<TopicHandleInterface>& impl = nullptr)
    : self{impl} {}

    /**
     * @brief Copy-constructor.
     */
    TopicHandle(const TopicHandle&) = default;

    /**
     * @brief Move-constructor.
     */
    TopicHandle(TopicHandle&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    TopicHandle& operator=(const TopicHandle&) = default;

    /**
     * @brief Move-assignment operator.
     */
    TopicHandle& operator=(TopicHandle&&) = default;

    /**
     * @brief Destructor.
     */
    ~TopicHandle() = default;

    /**
     * @brief Returns the name of the topic.
     */
    const std::string& name() const {
        return self->name();
    }

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
            GetArgOrDefault(ThreadPool{}, std::forward<Options>(opts)...),
            GetArgOrDefault(Ordering::Strict, std::forward<Options>(opts)...));
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
            GetArgOrDefault(std::vector<size_t>(), std::forward<Options>(opts)...));
    }

    /**
     * @brief Returns the list of PartitionInfo of the underlying topic.
     */
    const std::vector<PartitionInfo>& partitions() const {
        return self->partitions();
    }

    /**
     * @brief Return the Validator of the topic.
     */
    Validator validator() const {
        return self->validator();
    }

    /**
     * @brief Return the PartitionSelector of the topic.
     */
    PartitionSelector selector() const {
        return self->selector();
    }

    /**
     * @brief Return the Serializer of the topic.
     */
    Serializer serializer() const {
        return self->serializer();
    }

    /**
     * @brief Indicate to the partition servers that no more events will be produced
     * in this topic. This will make any attempt to consume events return events with
     * no metadata, no data, and an ID of NoMoreEvents.
     */
    void markAsComplete() const {
        self->markAsComplete();
    }

    /**
     * @brief Checks if the TopicHandle instance is valid.
     */
    operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    std::shared_ptr<TopicHandleInterface> self;

    /**
     * @brief Create a Producer object from the full
     * list of optional arguments.
     *
     * @param name Name of the Producer.
     * @param batch_size Batch size.
     * @param thread_pool Thread pool.
     * @param ordering Whether to enforce strict ordering.
     *
     * @return Producer instance.
     */
    Producer makeProducer(std::string_view name,
                          BatchSize batch_size,
                          ThreadPool thread_pool,
                          Ordering ordering) const {
        return self->makeProducer(name, batch_size, std::move(thread_pool), ordering);
    }

    /**
     * @brief Create a Consumer object from the full
     * list of optional and mandatory arguments.
     *
     * @param name Name of the Consumer.
     * @param batch_size Batch size.
     * @param thread_pool Thread pool.
     * @param data_broker Data broker.
     * @param data_selector Data selector.
     * @param targets Indices of the partitions to consumer from.
     *
     * @return Consumer instance.
     */
    Consumer makeConsumer(std::string_view name,
                          BatchSize batch_size,
                          ThreadPool thread_pool,
                          DataBroker data_broker,
                          DataSelector data_selector,
                          const std::vector<size_t>& targets) const {
        return self->makeConsumer(name, batch_size, thread_pool, data_broker, data_selector, targets);
    }

};

}

#endif
