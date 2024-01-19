/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "ServiceHandleImpl.hpp"
#include "PartitionInfoImpl.hpp"
#include "mofka/Validator.hpp"
#include "mofka/PartitionSelector.hpp"
#include "mofka/Serializer.hpp"
#include <string_view>

namespace mofka {

class TopicHandleImpl {

    public:

    std::string                m_name;
    SP<ServiceHandleImpl>      m_service;
    Validator                  m_validator;
    PartitionSelector          m_selector;
    Serializer                 m_serializer;
    std::vector<PartitionInfo> m_partitions;

    TopicHandleImpl() = default;

    TopicHandleImpl(std::string_view name,
                    SP<ServiceHandleImpl> service,
                    Validator validator,
                    PartitionSelector selector,
                    Serializer serializer,
                    std::vector<PartitionInfo> partitions)
    : m_name(name)
    , m_service(std::move(service))
    , m_validator(std::move(validator))
    , m_selector(std::move(selector))
    , m_serializer(std::move(serializer))
    , m_partitions(std::move(partitions)) {
        m_selector.setPartitions(m_partitions);
    }
};

}

#endif
