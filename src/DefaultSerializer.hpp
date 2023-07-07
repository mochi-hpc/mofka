/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DEFAULT_SERIALIZER_H
#define MOFKA_DEFAULT_SERIALIZER_H

#include "mofka/Serializer.hpp"
#include "mofka/Json.hpp"

namespace mofka {

class DefaultSerializer : public SerializerInterface {

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

    static std::shared_ptr<SerializerInterface> Create(const Metadata& metadata) {
        (void)metadata;
        return std::make_shared<DefaultSerializer>();
    }
};

}

#endif
