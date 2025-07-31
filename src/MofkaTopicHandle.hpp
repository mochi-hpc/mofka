/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include <diaspora/Producer.hpp>
#include <diaspora/Ordering.hpp>
#include <diaspora/Validator.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/TopicHandle.hpp>

#include "MofkaPartitionInfo.hpp"
#include "MofkaProducer.hpp"

#include <string_view>

namespace mofka {

namespace tl = thallium;

class MofkaDriver;

class MofkaTopicHandle : public diaspora::TopicHandleInterface,
                         public std::enable_shared_from_this<MofkaTopicHandle>  {

    public:

    tl::engine                                       m_engine;
    std::string                                      m_name;
    diaspora::Validator                              m_validator;
    diaspora::PartitionSelector                      m_selector;
    diaspora::Serializer                             m_serializer;
    std::vector<std::shared_ptr<MofkaPartitionInfo>> m_partitions;
    std::vector<diaspora::PartitionInfo>             m_partitions_info;
    std::shared_ptr<MofkaDriver>                     m_driver;

    tl::remote_procedure m_topic_mark_as_complete;

    MofkaTopicHandle() = default;

    MofkaTopicHandle(tl::engine engine,
                     std::string_view name,
                     diaspora::Validator validator,
                     diaspora::PartitionSelector selector,
                     diaspora::Serializer serializer,
                     std::vector<std::shared_ptr<MofkaPartitionInfo>> partitions,
                     std::shared_ptr<MofkaDriver> driver)
    : m_engine{std::move(engine)}
    , m_name(name)
    , m_validator(std::move(validator))
    , m_selector(std::move(selector))
    , m_serializer(std::move(serializer))
    , m_partitions(std::move(partitions))
    , m_driver(std::move(driver))
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

    std::shared_ptr<diaspora::ProducerInterface> makeProducer(
          std::string_view name,
          diaspora::BatchSize batch_size,
          diaspora::MaxNumBatches max_batch,
          diaspora::Ordering ordering,
          std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
          diaspora::Metadata options) override;

    std::shared_ptr<diaspora::ConsumerInterface> makeConsumer(
          std::string_view name,
          diaspora::BatchSize batch_size,
          diaspora::MaxNumBatches max_batch,
          std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
          diaspora::DataAllocator data_allocator,
          diaspora::DataSelector data_selector,
          const std::vector<size_t>& targets,
          diaspora::Metadata options) override;

    const std::vector<diaspora::PartitionInfo>& partitions() const override {
        return m_partitions_info;
    }

    std::shared_ptr<diaspora::DriverInterface> driver() const override;

    diaspora::Validator validator() const override {
        return m_validator;
    }

    diaspora::PartitionSelector selector() const override {
        return m_selector;
    }

    diaspora::Serializer serializer() const override {
        return m_serializer;
    }

    void markAsComplete() override;
};

}

#endif
