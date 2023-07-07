/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SIMPLE_SERIALIZER_IMPL_H
#define MOFKA_SIMPLE_SERIALIZER_IMPL_H

#include "mofka/Serializer.hpp"
#include "mofka/Json.hpp"

namespace mofka {

class SimpleSerializerImpl : public SerializerInterface {

    public:

    void serialize(Archive& archive, const Metadata& metadata) const override {
        auto& json = metadata.json();
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        json.Accept(writer);
        archive.write(buffer.GetString(), buffer.GetSize());
    }

    void deserialize(Archive& archive, Metadata& metadata) const override {
        metadata.json() = rapidjson::Document{};
        auto reader = [&](const void* data, std::size_t size) {
            metadata.json().Parse(static_cast<const char*>(data), size);
        };
        archive.feed(reader);
    }

    Metadata metadata() const override {
        return Metadata{"{}"};
    }

    void fromMetadata(const Metadata& metadata) override {
        (void)metadata;
    }

};

}

#endif
