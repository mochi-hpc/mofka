/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Serializer.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "DefaultSerializer.hpp"
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

std::unordered_map<std::string, std::function<std::shared_ptr<SerializerInterface>(const Metadata&)>>
    serializerFactories;

MOFKA_REGISTER_SERIALIZER(default, DefaultSerializer);

void Serializer::RegisterSerializerType(
        std::string_view name,
        std::function<std::shared_ptr<SerializerInterface>(const Metadata&)> ctor) {
    serializerFactories[std::string{name.data(), name.size()}] = std::move(ctor);
}

Serializer Serializer::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.IsObject()) {
        throw Exception(
                "Cannor create Serializer from Metadata: "
                "invalid Metadata (expected JSON object)");
    }
    if(!json.HasMember("__type__")) {
        return Serializer{};
    }
    auto& type = json["__type__"];
    if(!type.IsString()) {
        throw Exception(
                "Cannor create Serializer from Metadata: "
                "invalid __type__ in Metadata (expected string)");
    }
    auto type_str = std::string{type.GetString()};
    auto it = serializerFactories.find(type_str);
    if(it == serializerFactories.end()) {
        throw Exception(
                "Cannor create Serializer from Metadata: "
                "unknown Serializer type \"{}\"",
                type_str);
    }
    return (it->second)(metadata);
}

}
