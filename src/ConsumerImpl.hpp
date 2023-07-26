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
    UUID                             m_uuid;
    BatchSize                        m_batch_size;
    ThreadPool                       m_thread_pool;
    DataBroker                       m_data_broker;
    DataSelector                     m_data_selector;
    EventProcessor                   m_event_processor;
    std::vector<PartitionTargetInfo> m_targets;
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

    /**
     * Vector of eventuals that will be set on completion of the
     * ULTs that pull events from each target.
     */
    std::vector<thallium::eventual<void>> m_pulling_ult_completed;

    ConsumerImpl(std::string_view name,
                 BatchSize batch_size,
                 ThreadPool thread_pool,
                 DataBroker broker,
                 DataSelector selector,
                 std::vector<PartitionTargetInfo> targets,
                 std::shared_ptr<TopicHandleImpl> topic)
    : m_name(name)
    , m_uuid(UUID::generate())
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_data_broker(std::move(broker))
    , m_data_selector(std::move(selector))
    , m_targets(std::move(targets))
    , m_topic(std::move(topic)) {
        start();
    }

    ~ConsumerImpl() {
        join();
    }

    private:

    void start();
    void join();
    void pullFrom(
        const PartitionTargetInfo& target,
        thallium::eventual<void>& ev);
};

}

#endif
