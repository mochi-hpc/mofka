/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "mofka/Exception.hpp"
#include "mofka/Serializer.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultSerializer.hpp"
#include <fmt/format.h>
#include <unordered_map>

namespace mofka {

using SerializerImpl = SerializerInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Serializer);

Serializer::Serializer()
: self(std::make_shared<DefaultSerializer>()) {}

void Serializer::serialize(Archive& archive, const Metadata& metadata) const {
    self->serialize(archive, metadata);
}

void Serializer::deserialize(Archive& archive, Metadata& metadata) const {
    self->deserialize(archive, metadata);
}

Metadata Serializer::metadata() const {
    return self->metadata();
}

MOFKA_REGISTER_SERIALIZER(default, DefaultSerializer);

Serializer Serializer::FromMetadata(const Metadata& metadata) {
    const auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
                "Cannot create Serializer from Metadata: "
                "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("__type__")) {
        return Serializer{};
    }
    auto& type = json["__type__"];
    if(!type.is_string()) {
        throw Exception(
                "Cannot create Serializer from Metadata: "
                "invalid __type__ in Metadata (expected string)");
    }
    const auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<SerializerInterface> s = SerializerFactory::create(type_str, metadata);
    return Serializer(std::move(s));
}

}
