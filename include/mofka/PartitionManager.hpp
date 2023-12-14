/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_MANAGER_HPP
#define MOFKA_PARTITION_MANAGER_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Result.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Json.hpp>
#include <mofka/BatchSize.hpp>
#include <mofka/EventID.hpp>
#include <mofka/ConsumerHandle.hpp>
#include <mofka/Factory.hpp>

#include <thallium.hpp>
#include <unordered_map>
#include <string_view>
#include <functional>

namespace mofka {

/**
 * @brief Interface for topic backends. To build a new backend,
 * implement a class MyPartitionManager that inherits from PartitionManager, and put
 * MOFKA_REGISTER_PARTITION_MANAGER(mybackend, MyPartitionManager); in a cpp file
 * that includes your backend class' header file.
 */
class PartitionManager {

    public:

    /**
     * @brief Constructor.
     */
    PartitionManager() = default;

    /**
     * @brief Move-constructor.
     */
    PartitionManager(PartitionManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    PartitionManager(const PartitionManager&) = default;

    /**
     * @brief Move-assignment operator.
     */
    PartitionManager& operator=(PartitionManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    PartitionManager& operator=(const PartitionManager&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~PartitionManager() = default;

    /**
     * @brief Get the Metadata of the Validator associated with this topic.
     */
    virtual Metadata getValidatorMetadata() const = 0;

    /**
     * @brief Get the Metadata of the TargetSelector associated with this topic.
     */
    virtual Metadata getTargetSelectorMetadata() const = 0;

    /**
     * @brief Get the Metadata of the Serializer associated with this topic.
     */
    virtual Metadata getSerializerMetadata() const = 0;

    /**
     * @brief Receive a batch of events from a sender.
     *
     * @param producer_name Name of the producer.
     * @param num_events Number of events sent.
     * @param metadata_bulk_size Total size of the bulk handle holding metadata and sizes.
     * @param metadata_bulk_offset Offset at which to start in the bulk handle.
     * @param metadata_bulk Bulk handle holding metadata sizes and metadata.
     * @param data_bulk_size Total size of the bulk handle holding data and sizes.
     * @param data_bulk_offset Offset at which to start in the bulk handle.
     * @param data_bulk Bulk handle holding data sizes and metadata.
     *
     * The bulk handles are formatted to expose the metadata and data
     * as follows. If N is the number of events, then:
     * - the first N*sizeof(size_t) bytes contain metadata/data sizes;
     * - the next S bytes (sum of the above sizes) contain the metadata/data content.
     *
     * @return a Result containing the result.
     */
    virtual Result<EventID> receiveBatch(
        const thallium::endpoint& sender,
        const std::string& producer_name,
        size_t num_events,
        const BulkRef& metadata_bulk,
        const BulkRef& data_bulk) = 0;

    /**
     * @brief This function is used to wake up the topic manager to make
     * if check again the shouldStop() function of blocked ConsumerHandles.
     */
    virtual void wakeUp() = 0;

    /**
     * @brief Attach a ConsumerHandle to the topic, i.e. make the
     * PartitionManager feed the ConsumerHandle batches of events.
     *
     * The feedConsumer function should keep feeding the ConsumerHandle
     * with events (blocking if there is no new events yet) until its
     * feed() function returns false.
     *
     * Multiple ConsumderHandle may be fed in parallel. The PartitionManager
     * is responsible for feeding each event only once.
     *
     * @param consumerHandle ConsumerHandle to feed event batches.
     * @param bathSize batch size requested by the consumer.
     */
    virtual Result<void> feedConsumer(
        ConsumerHandle consumerHandle,
        BatchSize batchSize) = 0;

    /**
     * @brief Acknowledge that the specified consumer has consumed
     * events up to and including the specified event ID.
     */
    virtual Result<void> acknowledge(
        std::string_view consumer_name,
        EventID event_id) = 0;

    /**
     * @brief Fetch the data associated with a given series of DataDescriptors.
     *
     * @param descriptors Vector of DataDescriptor for the data to fetch.
     * @param bulk Bulk handle of the sender's memory.
     */
    virtual Result<std::vector<Result<void>>> getData(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& bulk) = 0;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual Result<bool> destroy() = 0;

};

using PartitionManagerFactory = Factory<PartitionManager,
    const thallium::engine&,
    const Metadata&,
    const Metadata&,
    const Metadata&,
    const Metadata&>;

#define MOFKA_REGISTER_PARTITION_MANAGER(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(PartitionManagerFactory, __type__, __name__)

} // namespace mofka

#endif
