/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_IMPL_H
#define MOFKA_CONSUMER_IMPL_H

#include "PimplUtil.hpp"
#include "MofkaPartitionInfo.hpp"
#include "Promise.hpp"

#include "MofkaTopicHandle.hpp"
#include "mofka/Consumer.hpp"
#include "mofka/UUID.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class MofkaTopicHandle;

class MofkaConsumer : public std::enable_shared_from_this<MofkaConsumer>,
                      public ConsumerInterface {

    friend class MofkaTopicHandle;

    #define MOFKA_MAGIC_NUMBER (*((uint64_t*)"matthieu"))

    public:

    uint64_t                            m_magic_number = MOFKA_MAGIC_NUMBER;
    thallium::engine                    m_engine;
    std::string                         m_name;
    BatchSize                           m_batch_size;
    ThreadPool                          m_thread_pool;
    DataBroker                          m_data_broker;
    DataSelector                        m_data_selector;
    EventProcessor                      m_event_processor;
    SP<MofkaTopicHandle>                m_topic;
    std::vector<SP<MofkaPartitionInfo>> m_partitions;

    std::string         m_self_addr;
    std::atomic<size_t> m_completed_partitions = 0;

    tl::remote_procedure m_consumer_request_events;
    tl::remote_procedure m_consumer_ack_event;
    tl::remote_procedure m_consumer_remove_consumer;
    tl::remote_procedure m_consumer_request_data;
    tl::remote_procedure m_consumer_recv_batch;

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
    std::deque<std::pair<Promise<Event>, Future<Event>>> m_futures;
    bool                                                 m_futures_credit;
    thallium::mutex                                      m_futures_mtx;

    MofkaConsumer(thallium::engine engine,
                  std::string_view name,
                  BatchSize batch_size,
                  ThreadPool thread_pool,
                  DataBroker broker,
                  DataSelector selector,
                  SP<MofkaTopicHandle> topic,
                  std::vector<SP<MofkaPartitionInfo>> partitions)
    : m_engine(std::move(engine))
    , m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_data_broker(std::move(broker))
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

    BatchSize batchSize() const override {
        return m_batch_size;
    }

    ThreadPool threadPool() const override {
        return m_thread_pool;
    }

    TopicHandle topic() const override;

    DataBroker dataBroker() const override {
        return m_data_broker;
    }

    DataSelector dataSelector() const override {
        return m_data_selector;
    }

    Future<Event> pull() override;

    void unsubscribe() override;

    private:

    void subscribe();

    void recvBatch(
        size_t target_info_index,
        size_t count,
        EventID firstID,
        const BulkRef &metadata_sizes,
        const BulkRef &metadata,
        const BulkRef &data_desc_sizes,
        const BulkRef &data_desc);

    Data requestData(
        SP<MofkaPartitionInfo> target,
        Metadata metadata,
        const DataDescriptor& descriptor);

    static void forwardBatchToConsumer(
            const thallium::request& req,
            intptr_t consumer_ctx,
            size_t target_info_index,
            size_t count,
            EventID firstID,
            const BulkRef &metadata_sizes,
            const BulkRef &metadata,
            const BulkRef &data_desc_sizes,
            const BulkRef &data_desc);
};

}

#endif
