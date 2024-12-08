/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "mofka/Exception.hpp"
#include "mofka/PartitionSelector.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultPartitionSelector.hpp"
#include <fmt/format.h>
#include <unordered_map>

namespace mofka {

using PartitionSelectorImpl = PartitionSelectorInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(PartitionSelector);

PartitionSelector::PartitionSelector()
: self(std::make_shared<DefaultPartitionSelector>()) {}

void PartitionSelector::setPartitions(const std::vector<PartitionInfo>& targets) {
    return self->setPartitions(targets);
}

size_t PartitionSelector::selectPartitionFor(const Metadata& metadata, std::optional<size_t> requested) {
    return self->selectPartitionFor(metadata, requested);
}

Metadata PartitionSelector::metadata() const {
    return self->metadata();
}

static std::unordered_map<std::string, std::function<std::shared_ptr<PartitionSelectorInterface>(const Metadata&)>>
    targetSelectorFactories;

MOFKA_REGISTER_PARTITION_SELECTOR(default, DefaultPartitionSelector);

PartitionSelector PartitionSelector::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("type")) {
        return PartitionSelector{};
    }
    auto& type = json["type"];
    if(!type.is_string()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid \"type\" field in Metadata (expected string)");
    }
    auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<PartitionSelectorInterface> ts = PartitionSelectorFactory::create(type_str, metadata);
    return ts;
}

PartitionSelector PartitionSelector::FromMetadata(const char* type, const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    auto md_copy = metadata;
    md_copy.json()["type"] = type;
    std::shared_ptr<PartitionSelectorInterface> ts = PartitionSelectorFactory::create(type, md_copy);
    return ts;
}

}
