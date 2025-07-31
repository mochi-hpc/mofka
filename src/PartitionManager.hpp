/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PARTITION_MANAGER_HPP
#define MOFKA_PARTITION_MANAGER_HPP

#include "Result.hpp"
#include "UUID.hpp"
#include "ConsumerHandle.hpp"

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Json.hpp>
#include <diaspora/BatchParams.hpp>
#include <diaspora/EventID.hpp>
#include <diaspora/Factory.hpp>

#include <bedrock/AbstractComponent.hpp>

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
    virtual Result<diaspora::EventID> receiveBatch(
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
        diaspora::BatchSize batchSize) = 0;

    /**
     * @brief Acknowledge that the specified consumer has consumed
     * events up to and including the specified event ID.
     */
    virtual Result<void> acknowledge(
        std::string_view consumer_name,
        diaspora::EventID event_id) = 0;

    /**
     * @brief Fetch the data associated with a given series of DataDescriptors.
     *
     * @param descriptors Vector of DataDescriptor for the data to fetch.
     * @param bulk Bulk handle of the sender's memory.
     */
    virtual Result<std::vector<Result<void>>> getData(
        const std::vector<diaspora::DataDescriptor>& descriptors,
        const BulkRef& bulk) = 0;

    /**
     * @brief Mark the partition as complete, i.e. it will not receive
     * any more events from producers.
     */
    virtual Result<void> markAsComplete() = 0;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a Result<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual Result<bool> destroy() = 0;

};

template <typename ManagerType>
struct PartitionDependencyRegistrar;

class PartitionManagerDependencyFactory {

    public:

    static inline std::vector<bedrock::Dependency> getDependencies(const std::string& type) {
        auto& factory = instance();
        auto it = factory.dependencies.find(type);
        if(it == factory.dependencies.end()) return {};
        return it->second;
    }

    template<typename T>
    friend struct PartitionDependencyRegistrar;

    static PartitionManagerDependencyFactory& instance();

    std::unordered_map<std::string,
                       std::vector<bedrock::Dependency>> dependencies;
};

template <typename ManagerType>
struct PartitionDependencyRegistrar {

    explicit PartitionDependencyRegistrar(
            const std::string& key,
            std::vector<bedrock::Dependency> deps)
    {
        PartitionManagerDependencyFactory::instance()
            .dependencies[key] = std::move(deps);
    }

};

using PartitionManagerFactory = diaspora::Factory<PartitionManager,
    const thallium::engine&,
    const std::string&,        /* topic name */
    const UUID&,               /* partition UUID */
    const diaspora::Metadata&, /* partition config */
    const bedrock::ResolvedDependencyMap&>;

#define MOFKA_REGISTER_PARTITION_MANAGER(__name__, __type__) \
    DIASPORA_REGISTER_IMPLEMENTATION_FOR(mofka, PartitionManagerFactory, __type__, __name__);

#define MOFKA_REGISTER_PARTITION_MANAGER_WITH_DEPENDENCIES(__name__, __type__, ...) \
    DIASPORA_REGISTER_IMPLEMENTATION_FOR(mofka, PartitionManagerFactory, __type__, __name__); \
    static ::mofka::PartitionDependencyRegistrar<__type__> \
        __mofkaDependencyRegistrarFor_ ## __type__ ## _ ## __name__{#__name__, {__VA_ARGS__}}

} // namespace mofka

#endif
