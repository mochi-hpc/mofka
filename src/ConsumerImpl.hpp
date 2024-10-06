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

#include "TopicHandleImpl.hpp"
#include "mofka/Consumer.hpp"
#include "mofka/UUID.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

class ClientImpl;

class ConsumerImpl : public std::enable_shared_from_this<ConsumerImpl> {

    friend class ClientImpl;

    public:

    thallium::engine                    m_engine;
    std::string                         m_name;
    BatchSize                           m_batch_size;
    ThreadPool                          m_thread_pool;
    DataBroker                          m_data_broker;
    DataSelector                        m_data_selector;
    EventProcessor                      m_event_processor;
    SP<TopicHandleImpl>                 m_topic;
    std::vector<SP<MofkaPartitionInfo>> m_partitions;

    std::string         m_self_addr;
    std::atomic<size_t> m_completed_partitions = 0;

    thallium::remote_procedure m_consumer_request_data;
    thallium::remote_procedure m_consumer_ack_event;

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

    ConsumerImpl(thallium::engine engine,
                 std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 DataBroker broker,
                 DataSelector selector,
                 SP<TopicHandleImpl> topic,
                 std::vector<SP<MofkaPartitionInfo>> partitions,
                 thallium::remote_procedure request_data,
                 thallium::remote_procedure ack_event)
    : m_engine(std::move(engine))
    , m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_data_broker(std::move(broker))
    , m_data_selector(std::move(selector))
    , m_topic(std::move(topic))
    , m_partitions(std::move(partitions))
    , m_self_addr(m_engine.self())
    , m_consumer_request_data{request_data}
    , m_consumer_ack_event{ack_event}
    {}

    ~ConsumerImpl() {
        unsubscribe();
    }

    void subscribe();

    void unsubscribe();

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
        SP<DataDescriptorImpl> descriptor);
};

}

#endif
