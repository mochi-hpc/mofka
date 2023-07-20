/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_IMPL_H
#define MOFKA_CONSUMER_IMPL_H

#include <thallium.hpp>
#include "TopicHandleImpl.hpp"
#include "PartitionTargetInfoImpl.hpp"
#include "BatchImpl.hpp"
#include "mofka/Consumer.hpp"
#include "mofka/UUID.hpp"
#include <string_view>
#include <queue>

namespace mofka {

class ConsumerImpl {

    public:

    std::string                      m_name;
    BatchSize                        m_batch_size;
    ThreadPool                       m_thread_pool;
    DataBroker                       m_data_broker;
    DataSelector                     m_data_selector;
    EventProcessor                   m_event_processor;
    std::shared_ptr<TopicHandleImpl> m_topic;

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
     * have been created by the consumer before th user had a chance
     * to call pull(). Hence if the consumer needs to issue a new
     * operation, it will push a new promise/future pair at the back
     * of the queue. If the user calls pull(), it will use the future
     * at in m_futures.front() (the oldest created by the consumer)
     * and take it off the queue. This is the symetric of the above.
     */
    std::deque<std::pair<Promise<Event>, Future<Event>>> m_futures;
    bool                                                 m_futures_credit;
    thallium::mutex                                      m_futures_mtx;

    ConsumerImpl(std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 DataBroker broker,
                 DataSelector selector,
                 std::shared_ptr<TopicHandleImpl> topic)
    : m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_data_broker(std::move(broker))
    , m_data_selector(std::move(selector))
    , m_topic(std::move(topic)) {}

};

}

#endif
