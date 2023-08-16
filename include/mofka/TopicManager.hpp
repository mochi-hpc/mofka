/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BACKEND_HPP
#define MOFKA_BACKEND_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/RequestResult.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Json.hpp>
#include <mofka/BatchSize.hpp>
#include <mofka/EventID.hpp>
#include <mofka/ConsumerHandle.hpp>

#include <thallium.hpp>
#include <unordered_map>
#include <string_view>
#include <functional>

/**
 * @brief Helper class to register backend types into the backend factory.
 */
template<typename TopicManagerType>
class __MofkaTopicManagerRegistration;

namespace mofka {

/**
 * @brief Interface for topic backends. To build a new backend,
 * implement a class MyTopicManager that inherits from TopicManager, and put
 * MOFKA_REGISTER_BACKEND(mybackend, MyTopicManager); in a cpp file
 * that includes your backend class' header file.
 */
class TopicManager {

    public:

    /**
     * @brief Constructor.
     */
    TopicManager() = default;

    /**
     * @brief Move-constructor.
     */
    TopicManager(TopicManager&&) = default;

    /**
     * @brief Copy-constructor.
     */
    TopicManager(const TopicManager&) = default;

    /**
     * @brief Move-assignment operator.
     */
    TopicManager& operator=(TopicManager&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    TopicManager& operator=(const TopicManager&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~TopicManager() = default;

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
     * @return a RequestResult containing the result.
     */
    virtual RequestResult<EventID> receiveBatch(
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
     * TopicManager feed the ConsumerHandle batches of events.
     *
     * The feedConsumer function should keep feeding the ConsumerHandle
     * with events (blocking if there is no new events yet) until its
     * feed() function returns false.
     *
     * Multiple ConsumderHandle may be fed in parallel. The TopicManager
     * is responsible for feeding each event only once.
     *
     * @param consumerHandle ConsumerHandle to feed event batches.
     * @param bathSize batch size requested by the consumer.
     */
    virtual RequestResult<void> feedConsumer(
        ConsumerHandle consumerHandle,
        BatchSize batchSize) = 0;

    /**
     * @brief Acknowledge that the specified consumer has consumed
     * events up to and including the specified event ID.
     */
    virtual RequestResult<void> acknowledge(
        std::string_view consumer_name,
        EventID event_id) = 0;

    /**
     * @brief Fetch the data associated with a given series of DataDescriptors.
     *
     * @param descriptors Vector of DataDescriptor for the data to fetch.
     * @param bulk Bulk handle of the sender's memory.
     */
    virtual RequestResult<void> getData(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& bulk) = 0;

    /**
     * @brief Destroys the underlying topic.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual RequestResult<bool> destroy() = 0;

};

/**
 * @brief The TopicFactory contains functions to create Topic objects.
 */
class TopicFactory {

    template<typename TopicManagerType>
    friend class ::__MofkaTopicManagerRegistration;

    public:

    TopicFactory() = delete;

    /**
     * @brief Creates a topic and returns a unique_ptr to the created instance.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration to pass to the backend's create function.
     * @param validator Metadata of the topic's validator.
     * @param serializer Metadata of the topic's serializer.
     *
     * @return a unique_ptr to the created TopicManager.
     */
    static std::unique_ptr<TopicManager> createTopic(
            std::string_view backend_name,
            const thallium::engine& engine,
            const Metadata& config,
            const Metadata& validator,
            const Metadata& selector,
            const Metadata& serializer);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<TopicManager>(
                    const thallium::engine&,
                    const Metadata&,
                    const Metadata&,
                    const Metadata&,
                    const Metadata&)>> create_fn;
};

} // namespace mofka


#define MOFKA_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __MofkaTopicManagerRegistration<__backend_type> __mofka ## __backend_name ## _backend( #__backend_name )

template<typename TopicManagerType>
class __MofkaTopicManagerRegistration {

    public:

    __MofkaTopicManagerRegistration(std::string_view backend_name)
    {
        mofka::TopicFactory::create_fn.emplace(
            backend_name, [](const thallium::engine& engine,
                             const mofka::Metadata& config,
                             const mofka::Metadata& validator,
                             const mofka::Metadata& selector,
                             const mofka::Metadata& serializer) {
                return TopicManagerType::create(engine, config, validator, selector, serializer);
            });
    }
};

#endif
