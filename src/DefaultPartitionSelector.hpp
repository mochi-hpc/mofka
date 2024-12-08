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

    size_t selectPartitionFor(const Metadata& metadata, std::optional<size_t> requested) override {
        (void)metadata;
        if(m_targets.size() == 0)
            throw Exception("PartitionSelector has no target to select from");
        if(requested.has_value()) {
            size_t req = requested.value();
            if(req >= m_targets.size()) {
                throw Exception("Requested partition is out of range");
            }
        }
        auto ret = m_index;
        m_index += 1;
        m_index %= m_targets.size();
        return ret;
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
