/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HANDLE_HPP
#define MOFKA_CONSUMER_HANDLE_HPP

#include <mofka/BulkRef.hpp>
#include <thallium.hpp>
#include <memory>

namespace mofka {

class ConsumerHandleImpl;
class ProviderImpl;

/**
 * @brief A ConsumerHandle is an object used by a TopicManager
 * to send events to a Consumer.
 */
class ConsumerHandle {

    friend class ProviderImpl;

    public:

    /**
     * @brief Constructor. The resulting ConsumerHandle handle will be invalid.
     */
    ConsumerHandle();

    /**
     * @brief Copy-constructor.
     */
    ConsumerHandle(const ConsumerHandle&);

    /**
     * @brief Move-constructor.
     */
    ConsumerHandle(ConsumerHandle&&);

    /**
     * @brief Copy-assignment operator.
     */
    ConsumerHandle& operator=(const ConsumerHandle&);

    /**
     * @brief Move-assignment operator.
     */
    ConsumerHandle& operator=(ConsumerHandle&&);

    /**
     * @brief Destructor.
     */
    ~ConsumerHandle();

    /**
     * @brief Returns the name of the consumer.
     */
    const std::string& name() const;

    /**
     * @brief Feed a batch of events to the ConsumerHandle.
     * This
     *
     * @param count Number of events.
     * @param metadata_sizes Bulk wrapping the metadata sizes (count*size_t).
     * @param metadata Bulk wrapping the metadata.
     * @param data_desc_sizes Bulk wrapping data descriptor sizes (count*size_t).
     * @param data_desc Bulk wrapping data descriptors.
     *
     * @return true if the TopicManager should continue feeding this
     * ConsumerHandle, false if it should stop.
     */
    bool feed(size_t count,
              const BulkRef& metadata_sizes,
              const BulkRef& metadata,
              const BulkRef& data_desc_sizes,
              const BulkRef& data_desc);

    /**
     * @brief Checks if the ConsumerHandle instance is valid.
     */
    operator bool() const;

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a ConsumerHandle instance.
     *
     * @param impl Pointer to implementation.
     */
    ConsumerHandle(const std::shared_ptr<ConsumerHandleImpl>& impl);

    std::shared_ptr<ConsumerHandleImpl> self;
};

}

#endif
