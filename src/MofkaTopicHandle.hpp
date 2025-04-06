/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "MofkaPartitionInfo.hpp"
#include "MofkaProducer.hpp"

#include "mofka/Producer.hpp"
#include "mofka/Ordering.hpp"
#include "mofka/Validator.hpp"
#include "mofka/PartitionSelector.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/TopicHandle.hpp"

#include <string_view>

namespace mofka {

namespace tl = thallium;

class MofkaTopicHandle : public std::enable_shared_from_this<MofkaTopicHandle>,
                         public TopicHandleInterface  {

    public:

    tl::engine                          m_engine;
    std::string                         m_name;
    Validator                           m_validator;
    PartitionSelector                   m_selector;
    Serializer                          m_serializer;
    std::vector<SP<MofkaPartitionInfo>> m_partitions;
    std::vector<PartitionInfo>          m_partitions_info;

    tl::remote_procedure m_topic_mark_as_complete;

    MofkaTopicHandle() = default;

    MofkaTopicHandle(tl::engine engine,
                     std::string_view name,
                     Validator validator,
                     PartitionSelector selector,
                     Serializer serializer,
                     std::vector<SP<MofkaPartitionInfo>> partitions)
    : m_engine{std::move(engine)}
    , m_name(name)
    , m_validator(std::move(validator))
    , m_selector(std::move(selector))
    , m_serializer(std::move(serializer))
    , m_partitions(std::move(partitions))
    , m_topic_mark_as_complete{m_engine.define("mofka_topic_mark_as_complete")} {
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
          MaxBatch max_batch,
          ThreadPool thread_pool,
          Ordering ordering,
          Metadata options) const override;

    Consumer makeConsumer(
          std::string_view name,
          BatchSize batch_size,
          MaxBatch max_batch,
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
