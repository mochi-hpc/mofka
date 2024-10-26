/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_KAFKA_TOPIC_HANDLE_IMPL_H

#include "mofka/Producer.hpp"
#include "mofka/Ordering.hpp"
#include "mofka/Validator.hpp"
#include "mofka/PartitionSelector.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/TopicHandle.hpp"

#include "KafkaPartitionInfo.hpp"

#include <string_view>

namespace mofka {

namespace tl = thallium;

class KafkaDriverImpl;

class KafkaTopicHandle : public std::enable_shared_from_this<KafkaTopicHandle>,
                         public TopicHandleInterface  {

    public:

    std::shared_ptr<KafkaDriverImpl>                 m_driver;
    std::string                                      m_name;
    Validator                                        m_validator;
    PartitionSelector                                m_selector;
    Serializer                                       m_serializer;
    std::vector<std::shared_ptr<KafkaPartitionInfo>> m_partitions;
    std::vector<PartitionInfo>                       m_partitions_info;

    KafkaTopicHandle(std::shared_ptr<KafkaDriverImpl> driver,
                     std::string_view name,
                     Validator validator,
                     PartitionSelector selector,
                     Serializer serializer,
                     std::vector<std::shared_ptr<KafkaPartitionInfo>> partitions)
    : m_driver{driver}
    , m_name(name)
    , m_validator(std::move(validator))
    , m_selector(std::move(selector))
    , m_serializer(std::move(serializer))
    , m_partitions(std::move(partitions))
    {
        m_partitions_info.reserve(m_partitions.size());
        for(auto& p : m_partitions) {
            m_partitions_info.push_back(p->toPartitionInfo());
        }
        m_selector.setPartitions(m_partitions_info);
    }

    const std::string& name() const override {
        return m_name;
    }

    Producer makeProducer(
          std::string_view name,
          BatchSize batch_size,
          ThreadPool thread_pool,
          Ordering ordering,
          Metadata options) const override;

    Consumer makeConsumer(
          std::string_view name,
          BatchSize batch_size,
          ThreadPool thread_pool,
          DataBroker data_broker,
          DataSelector data_selector,
          const std::vector<size_t>& targets,
          Metadata options) const override;

    const std::vector<PartitionInfo>& partitions() const override {
        return m_partitions_info;
    }

    Validator validator() const override {
        return m_validator;
    }

    PartitionSelector selector() const override {
        return m_selector;
    }

    Serializer serializer() const override {
        return m_serializer;
    }

    void markAsComplete() const override;
};

}

#endif
