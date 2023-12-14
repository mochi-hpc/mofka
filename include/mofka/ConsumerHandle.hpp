/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_HANDLE_HPP
#define MOFKA_CONSUMER_HANDLE_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/BulkRef.hpp>
#include <mofka/EventID.hpp>

#include <thallium.hpp>
#include <memory>

namespace mofka {

class ConsumerHandleImpl;
class ProviderImpl;

/**
 * @brief A ConsumerHandle is an object used by a PartitionManager
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
     *
     * @param count Number of events.
     * @param firstID ID of the first event in the batch.
     * @param metadata_sizes Bulk wrapping the metadata sizes (count*size_t).
     * @param metadata Bulk wrapping the metadata.
     * @param data_desc_sizes Bulk wrapping data descriptor sizes (count*size_t).
     * @param data_desc Bulk wrapping data descriptors.
     */
    void feed(size_t count,
              EventID firstID,
              const BulkRef& metadata_sizes,
              const BulkRef& metadata,
              const BulkRef& data_desc_sizes,
              const BulkRef& data_desc);

    /**
     * @brief Check if we should stop feeding the ConsumerHandle.
     */
    bool shouldStop() const;

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
