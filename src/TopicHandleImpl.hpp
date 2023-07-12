/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_TOPIC_HANDLE_IMPL_H
#define MOFKA_TOPIC_HANDLE_IMPL_H

#include "ServiceHandleImpl.hpp"
#include "PartitionTargetInfoImpl.hpp"
#include "mofka/Validator.hpp"
#include "mofka/TargetSelector.hpp"
#include "mofka/Serializer.hpp"
#include <string_view>

namespace mofka {

class TopicHandleImpl {

    public:

    std::string                        m_name;
    std::shared_ptr<ServiceHandleImpl> m_service;
    Validator                          m_validator;
    TargetSelector                     m_selector;
    Serializer                         m_serializer;

    TopicHandleImpl() = default;

    TopicHandleImpl(std::string_view name,
                    std::shared_ptr<ServiceHandleImpl> service,
                    Validator validator,
                    TargetSelector selector,
                    Serializer serializer)
    : m_name(name)
    , m_service(std::move(service))
    , m_validator(std::move(validator))
    , m_selector(std::move(selector))
    , m_serializer(std::move(serializer)) {
        const auto& mofka_phs = m_service->m_mofka_phs;
        std::vector<PartitionTargetInfo> targets;
        targets.reserve(mofka_phs.size());
        for(const auto& p : mofka_phs) {
            auto target_info = std::make_shared<PartitionTargetInfoImpl>(
                UUID::from_string("00000000-0000-0000-0000-000000000000"), // TODO change to actual UUID
                static_cast<std::string>(p),
                p.provider_id()
            );
            targets.push_back(target_info);
        }
        m_selector.setTargets(targets);
    }
};

}

#endif
