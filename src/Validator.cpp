/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Validator.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultValidator.hpp"
#include <fmt/format.h>
#include <unordered_map>

namespace mofka {

using ValidatorImpl = ValidatorInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Validator);

Validator::Validator()
: self(std::make_shared<DefaultValidator>()) {}

void Validator::validate(const Metadata& metadata, const Data& data) const {
    self->validate(metadata, data);
}

Metadata Validator::metadata() const {
    return self->metadata();
}

static std::unordered_map<std::string, std::function<std::shared_ptr<ValidatorInterface>(const Metadata&)>>
    validatorFactories;

MOFKA_REGISTER_VALIDATOR(default, DefaultValidator);

void Validator::RegisterValidatorType(
        std::string_view name,
        std::function<std::shared_ptr<ValidatorInterface>(const Metadata&)> ctor) {
    validatorFactories[std::string{name.data(), name.size()}] = std::move(ctor);
}

Validator Validator::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.IsObject()) {
        throw Exception(
            "Cannor create Validator from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.HasMember("__type__")) {
        return Validator{};
    }
    auto& type = json["__type__"];
    if(!type.IsString()) {
        throw Exception(
            "Cannor create Validator from Metadata: "
            "invalid __type__ in Metadata (expected string)");
    }
    auto type_str = std::string{type.GetString()};
    auto it = validatorFactories.find(type_str);
    if(it == validatorFactories.end()) {
        throw Exception(fmt::format(
            "Cannor create Validator from Metadata: "
            "unknown Validator type \"{}\"",
            type_str));
    }
    return (it->second)(metadata);
}

}
