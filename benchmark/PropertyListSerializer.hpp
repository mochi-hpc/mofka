#ifndef PROPERTY_LIST_SERIALIZER_H
#define PROPERTY_LIST_SERIALIZER_H

#include <mofka/Metadata.hpp>
#include <mofka/Serializer.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <iostream>
#include <chrono>
#include <random>
#include <mpi.h>


/**
 * @brief This class is a custom serializer that takes into account
 * the knowledge of the expected properties in an event as well as the maximum
 * value size of these properties. It will serialize/deserialize only the values
 * instead of serializing both keys and values, and will use the smallest
 * possible integer size to store the size of these values.
 */
class PropertyListSerializer : public mofka::SerializerInterface {

    using json = nlohmann::json;

    std::vector<std::string> m_properties;
    size_t                   m_maxValSize;

    public:

    PropertyListSerializer(std::vector<std::string> properties,
                           size_t maxValSize = std::numeric_limits<size_t>::max())
    : m_properties(std::move(properties))
    , m_maxValSize(maxValSize) {
        std::sort(m_properties.begin(), m_properties.end());
    }

    void serialize(mofka::Archive& archive, const mofka::Metadata& metadata) const override {
        for(const auto& p : m_properties) {
            const auto& v = metadata.json()[p].get_ref<const json::string_t&>();
            if(m_maxValSize <= std::numeric_limits<uint8_t>::max()) {
                uint8_t s = v.size();
                archive.write(&s, sizeof(s));
            } else if(m_maxValSize <= std::numeric_limits<uint16_t>::max()) {
                uint16_t s = v.size();
                archive.write(&s, sizeof(s));
            } else if(m_maxValSize <= std::numeric_limits<uint8_t>::max()) {
                uint32_t s = v.size();
                archive.write(&s, sizeof(s));
            } else {
                uint64_t s = v.size();
                archive.write(&s, sizeof(s));
            }
            archive.write(v.data(), v.size());
        }
    }

    void deserialize(mofka::Archive& archive, mofka::Metadata& metadata) const override {
        metadata = mofka::Metadata{json::object()};
        for(const auto& p : m_properties) {
            std::string v;
            if(m_maxValSize <= std::numeric_limits<uint8_t>::max()) {
                uint8_t s;
                archive.read(&s, sizeof(s));
                v.resize(s);
            } else if(m_maxValSize <= std::numeric_limits<uint16_t>::max()) {
                uint16_t s;
                archive.read(&s, sizeof(s));
                v.resize(s);
            } else if(m_maxValSize <= std::numeric_limits<uint8_t>::max()) {
                uint32_t s;
                archive.read(&s, sizeof(s));
                v.resize(s);
            } else {
                uint64_t s;
                archive.read(&s, sizeof(s));
                v.resize(s);
            }
            archive.read(const_cast<char*>(v.data()), v.size());
            metadata.json()[p] = std::move(v);
        }
    }

    mofka::Metadata metadata() const override {
        json meta = json::object();
        meta["type"] = "property_list_serializer";
        meta["properties"] = json::array();
        auto& properties = meta["properties"];
        for(auto& p : m_properties) {
            properties.push_back(p);
        }
        return meta;
    }

    static std::unique_ptr<SerializerInterface> create(const mofka::Metadata& metadata) {
        std::vector<std::string> properties;
        for(auto& p : metadata.json()["properties"]) {
            properties.push_back(p.get<std::string>());
        }
        return std::make_unique<PropertyListSerializer>(std::move(properties));
    }
};

#endif
