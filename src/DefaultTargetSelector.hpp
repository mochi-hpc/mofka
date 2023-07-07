/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DEFAULT_TARGET_SELECTOR_H
#define MOFKA_DEFAULT_TARGET_SELECTOR_H

#include "mofka/Metadata.hpp"
#include "mofka/TargetSelector.hpp"
#include "mofka/Json.hpp"
#include "MetadataImpl.hpp"
#include "RapidJsonUtil.hpp"

namespace mofka {

class DefaultTargetSelector : public TargetSelectorInterface {

    public:

    void setTargets(const std::vector<PartitionTargetInfo>& targets) override {
        m_targets = targets;
    }

    PartitionTargetInfo selectTargetFor(const Metadata& metadata) override {
        (void)metadata;
        if(m_targets.size() == 0)
            throw Exception("TargetSelector has no target to select from");
        if(m_index >= m_targets.size()) m_index = m_index % m_targets.size();
        m_index += 1;
        return m_targets.at(m_index-1);
    }

    Metadata metadata() const override {
        return Metadata{"{}"};
    }

    static std::shared_ptr<TargetSelectorInterface> Create(const Metadata& metadata) {
        (void)metadata;
        return std::make_shared<DefaultTargetSelector>();
    }

    size_t                           m_index = 0;
    std::vector<PartitionTargetInfo> m_targets;
};

}

#endif
