/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/TargetSelector.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultTargetSelector.hpp"
#include <fmt/format.h>
#include <unordered_map>

namespace mofka {

using TargetSelectorImpl = TargetSelectorInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(TargetSelector);

TargetSelector::TargetSelector()
: self(std::make_shared<DefaultTargetSelector>()) {}

void TargetSelector::setTargets(const std::vector<PartitionTargetInfo>& targets) {
    return self->setTargets(targets);
}

PartitionTargetInfo TargetSelector::selectTargetFor(const Metadata& metadata) {
    return self->selectTargetFor(metadata);
}

Metadata TargetSelector::metadata() const {
    return self->metadata();
}

static std::unordered_map<std::string, std::function<std::shared_ptr<TargetSelectorInterface>(const Metadata&)>>
    targetSelectorFactories;

MOFKA_REGISTER_TARGET_SELECTOR(default, DefaultTargetSelector);

void TargetSelector::RegisterTargetSelectorType(
        std::string_view name,
        std::function<std::shared_ptr<TargetSelectorInterface>(const Metadata&)> ctor) {
    targetSelectorFactories[std::string{name.data(), name.size()}] = std::move(ctor);
}

TargetSelector TargetSelector::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.IsObject()) {
        throw Exception(
            "Cannor create TargetSelector from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.HasMember("__type__")) {
        return TargetSelector{};
    }
    auto& type = json["__type__"];
    if(!type.IsString()) {
        throw Exception(
            "Cannor create TargetSelector from Metadata: "
            "invalid __type__ in Metadata (expected string)");
    }
    auto type_str = std::string{type.GetString()};
    auto it = targetSelectorFactories.find(type_str);
    if(it == targetSelectorFactories.end()) {
        throw Exception(fmt::format(
            "Cannor create TargetSelector from Metadata: "
            "unknown TargetSelector type \"{}\"",
            type_str));
    }
    return (it->second)(metadata);
}

}
