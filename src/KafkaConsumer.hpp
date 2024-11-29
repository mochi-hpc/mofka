/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_CONSUMER_IMPL_H
#define MOFKA_KAFKA_CONSUMER_IMPL_H

#include "PimplUtil.hpp"
#include "KafkaPartitionInfo.hpp"
#include "Promise.hpp"

#include "KafkaTopicHandle.hpp"
#include "mofka/Consumer.hpp"
#include "mofka/UUID.hpp"

#include <thallium.hpp>
#include <string_view>
#include <queue>
#include <librdkafka/rdkafka.h>

namespace mofka {

namespace tl = thallium;

class KafkaTopicHandle;

class KafkaConsumer : public std::enable_shared_from_this<KafkaConsumer>,
                      public ConsumerInterface {

    friend class KafkaTopicHandle;

    public:

    std::shared_ptr<rd_kafka_t>         m_kafka_consumer;
    std::string                         m_name;
    BatchSize                           m_batch_size;
    ThreadPool                          m_thread_pool;
    DataBroker                          m_data_broker;
    DataSelector                        m_data_selector;
    EventProcessor                      m_event_processor;
    SP<KafkaTopicHandle>                m_topic;
    std::vector<SP<KafkaPartitionInfo>> m_partitions;

    std::atomic<bool>  m_should_stop = true;
    tl::eventual<void> m_poll_ult_stopped;

    std::atomic<size_t> m_completed_partitions = 0;
    std::deque<std::variant<Event,Exception>> m_events;
    thallium::mutex                           m_events_mtx;
    thallium::condition_variable              m_events_cv;

    KafkaConsumer(std::string_view name,
                  BatchSize batch_size,
                  ThreadPool thread_pool,
                  DataBroker broker,
                  DataSelector selector,
                  SP<KafkaTopicHandle> topic,
                  std::vector<SP<KafkaPartitionInfo>> partitions,
                  std::shared_ptr<rd_kafka_t> kcons)
    : m_kafka_consumer{std::move(kcons)}
    , m_name(name)
    , m_batch_size(batch_size)
    , m_thread_pool(std::move(thread_pool))
    , m_data_broker(std::move(broker))
    , m_data_selector(std::move(selector))
    , m_topic(std::move(topic))
    , m_partitions(std::move(partitions))
    {}

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

    void handleReceivedMessage(rd_kafka_message_t* msg);

};

}

#endif
