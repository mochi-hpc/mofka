/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_IMPL_H
#define MOFKA_CONSUMER_IMPL_H

#include <mofka/MofkaPartitionInfo.hpp>
#include <mofka/MofkaTopicHandle.hpp>
#include <mofka/MofkaDriver.hpp>
#include <mofka/UUID.hpp>
#include <mofka/BulkRef.hpp>
#include <mofka/Promise.hpp>

#include <diaspora/Consumer.hpp>

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class MofkaTopicHandle;

class MofkaConsumer : public diaspora::ConsumerInterface {

    friend class MofkaTopicHandle;

    #define MOFKA_MAGIC_NUMBER (*((uint64_t*)"matthieu"))

    public:

    uint64_t                                         m_magic_number = MOFKA_MAGIC_NUMBER;
    thallium::engine                                 m_engine;
    std::string                                      m_name;
    diaspora::BatchSize                              m_batch_size;
    diaspora::MaxNumBatches                          m_max_batch;
    std::shared_ptr<diaspora::ThreadPoolInterface>   m_thread_pool;
    diaspora::DataAllocator                          m_data_allocator;
    diaspora::DataSelector                           m_data_selector;
    diaspora::EventProcessor                         m_event_processor;
    std::shared_ptr<MofkaTopicHandle>                m_topic;
    std::vector<std::shared_ptr<MofkaPartitionInfo>> m_partitions;

    std::string         m_self_addr;
    std::atomic<size_t> m_completed_partitions = 0;

    tl::remote_procedure m_consumer_request_events;
    tl::remote_procedure m_consumer_ack_event;
    tl::remote_procedure m_consumer_remove_consumer;
    tl::remote_procedure m_consumer_request_data;
    tl::remote_procedure m_consumer_recv_batch;

    std::shared_ptr<MofkaConsumer> shared_from_this_mofka() {
        return std::dynamic_pointer_cast<MofkaConsumer>(shared_from_this());
    }

    /* The futures/promises queue works as follows:
     *
     * If m_futures_credit is true, it means the futures in the queue
     * have been created by the user via pull() operations. Hence if
     * the consumer needs to issue a new operation, it will use the
     * promise in m_futures.front() (the oldest created by the user)
     * and take it off the queue. If the user calls pull(), it simply
     * append a new promise/future pair at the back of the queue.
     *
     * If m_future_credit is false, it means the future in the queue
     * have been created by the consumer before the user had a chance
     * to call pull(). Hence if the consumer needs to issue a new
     * operation, it will push a new promise/future pair at the back
     * of the queue. If the user calls pull(), it will use the future
     * at in m_futures.front() (the oldest created by the consumer)
     * and take it off the queue. This is the symetric of the above.
     */
    std::deque<std::pair<Promise<diaspora::Event>,
                         diaspora::Future<diaspora::Event>>> m_futures;
    bool                                                     m_futures_credit;
    thallium::mutex                                          m_futures_mtx;

    MofkaConsumer(thallium::engine engine,
                  std::string_view name,
                  diaspora::BatchSize batch_size,
                  diaspora::MaxNumBatches max_batch,
                  std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                  diaspora::DataAllocator allocator,
                  diaspora::DataSelector selector,
                  std::shared_ptr<MofkaTopicHandle> topic,
                  std::vector<std::shared_ptr<MofkaPartitionInfo>> partitions)
    : m_engine(std::move(engine))
    , m_name(name)
    , m_batch_size(batch_size)
    , m_max_batch(max_batch)
    , m_thread_pool(thread_pool ? std::move(thread_pool) : topic->driver()->defaultThreadPool())
    , m_data_allocator(std::move(allocator))
    , m_data_selector(std::move(selector))
    , m_topic(std::move(topic))
    , m_partitions(std::move(partitions))
    , m_self_addr(m_engine.self())
    , m_consumer_request_events(m_engine.define("mofka_consumer_request_events"))
    , m_consumer_ack_event(m_engine.define("mofka_consumer_ack_event"))
    , m_consumer_remove_consumer(m_engine.define("mofka_consumer_remove_consumer"))
    , m_consumer_request_data(m_engine.define("mofka_consumer_request_data"))
    , m_consumer_recv_batch(
        m_engine.define("mofka_consumer_recv_batch",
                        forwardBatchToConsumer,
                        0,
                        m_engine.get_progress_pool()))
    {}

    ~MofkaConsumer() {
        m_magic_number = 0;
    }

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_batch;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    const diaspora::DataAllocator& dataAllocator() const override {
        return m_data_allocator;
    }

    const diaspora::DataSelector& dataSelector() const override {
        return m_data_selector;
    }

    diaspora::Future<diaspora::Event> pull() override;

    void unsubscribe() override;

    void process(diaspora::EventProcessor processor,
                 std::shared_ptr<diaspora::ThreadPoolInterface> threadPool,
                 diaspora::NumEvents maxEvents) override {
        // TODO
        (void)processor;
        (void)threadPool;
        (void)maxEvents;
        throw diaspora::Exception{"MofkaConsumer::process not implemented"};
    }

    private:

    void subscribe();

    void partitionCompleted();

    void recvBatch(
        const tl::request& req,
        size_t target_info_index,
        size_t count,
        diaspora::EventID firstID,
        const BulkRef &metadata_sizes,
        const BulkRef &metadata,
        const BulkRef &data_desc_sizes,
        const BulkRef &data_desc);

    diaspora::DataView requestData(
        std::shared_ptr<MofkaPartitionInfo> target,
        diaspora::Metadata metadata,
        const diaspora::DataDescriptor& descriptor);

    static void forwardBatchToConsumer(
            const thallium::request& req,
            intptr_t consumer_ctx,
            size_t target_info_index,
            size_t count,
            diaspora::EventID firstID,
            const BulkRef &metadata_sizes,
            const BulkRef &metadata,
            const BulkRef &data_desc_sizes,
            const BulkRef &data_desc);
};

}

#endif
