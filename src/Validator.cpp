/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
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

MOFKA_REGISTER_VALIDATOR(default, DefaultValidator);

Validator Validator::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create Validator from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("__type__")) {
        return Validator{};
    }
    auto& type = json["__type__"];
    if(!type.is_string()) {
        throw Exception(
            "Cannot create Validator from Metadata: "
            "invalid __type__ in Metadata (expected string)");
    }
    auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<ValidatorInterface> v = ValidatorFactory::create(type_str, metadata);
    return v;
}

}
