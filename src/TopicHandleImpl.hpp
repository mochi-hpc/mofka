/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "ServiceHandleImpl.hpp"
#include "MofkaPartitionInfo.hpp"
#include "mofka/Validator.hpp"
#include "mofka/PartitionSelector.hpp"
#include "mofka/Serializer.hpp"
#include <string_view>

namespace mofka {

namespace tl = thallium;

class TopicHandleImpl {

    public:

    tl::engine                          m_engine;
    std::string                         m_name;
    SP<ServiceHandleImpl>               m_service;
    Validator                           m_validator;
    PartitionSelector                   m_selector;
    Serializer                          m_serializer;
    std::vector<SP<MofkaPartitionInfo>> m_partitions;
    std::vector<PartitionInfo>          m_partitions_info;

    tl::remote_procedure m_topic_mark_as_complete;

    TopicHandleImpl() = default;

    TopicHandleImpl(tl::engine engine,
                    std::string_view name,
                    SP<ServiceHandleImpl> service,
                    Validator validator,
                    PartitionSelector selector,
                    Serializer serializer,
                    std::vector<SP<MofkaPartitionInfo>> partitions)
    : m_engine{std::move(engine)}
    , m_name(name)
    , m_service(std::move(service))
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
};

}

#endif
