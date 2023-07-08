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
        const auto& str = metadata.string();
        size_t s = str.size();
        archive.write(&s, sizeof(s));
        archive.write(str.data(), s);
    }

    void deserialize(Archive& archive, Metadata& metadata) const override {
        auto& str = metadata.string();
        size_t s = 0;
        archive.read(&s, sizeof(s));
        str.resize(s);
        archive.read(const_cast<char*>(str.data()), s);
    }

    Metadata metadata() const override {
        return Metadata{"{\"type\":\"default\"}"};
    }

    static std::shared_ptr<SerializerInterface> Create(const Metadata& metadata) {
        (void)metadata;
        return std::make_shared<DefaultSerializer>();
    }
};

}

#endif
