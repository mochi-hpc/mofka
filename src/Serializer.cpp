/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Serializer.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "SimpleSerializerImpl.hpp"

namespace mofka {

using SerializerImpl = SerializerInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Serializer);

Serializer::Serializer()
: self(std::make_shared<SimpleSerializerImpl>()) {}

void Serializer::serialize(Archive& archive, const Metadata& metadata) const {
    self->serialize(archive, metadata);
}

void Serializer::deserialize(Archive& archive, Metadata& metadata) const {
    self->deserialize(archive, metadata);
}

Metadata Serializer::metadata() const {
    return self->metadata();
}

void Serializer::fromMetadata(const Metadata& metadata) {
    self->fromMetadata(metadata);
}

}
