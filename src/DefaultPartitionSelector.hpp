/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DEFAULT_PARTITION_SELECTOR_H
#define MOFKA_DEFAULT_PARTITION_SELECTOR_H

#include "mofka/Metadata.hpp"
#include "mofka/PartitionSelector.hpp"
#include "mofka/Json.hpp"
#include "MetadataImpl.hpp"
#include "JsonUtil.hpp"

namespace mofka {

class DefaultPartitionSelector : public PartitionSelectorInterface {

    public:

    void setPartitions(const std::vector<PartitionInfo>& targets) override {
        m_targets = targets;
    }

    PartitionInfo selectPartitionFor(const Metadata& metadata) override {
        (void)metadata;
        if(m_targets.size() == 0)
            throw Exception("PartitionSelector has no target to select from");
        if(m_index >= m_targets.size()) m_index = m_index % m_targets.size();
        m_index += 1;
        return m_targets.at(m_index-1);
    }

    Metadata metadata() const override {
        return Metadata{"{\"type\":\"default\"}"};
    }

    static std::unique_ptr<PartitionSelectorInterface> create(const Metadata& metadata) {
        (void)metadata;
        return std::make_unique<DefaultPartitionSelector>();
    }

    size_t                     m_index = 0;
    std::vector<PartitionInfo> m_targets;
};

}

#endif
