#ifndef METADATA_GENERATOR_H
#define METADATA_GENERATOR_H

#include "StringGenerator.hpp"
#include <mofka/Metadata.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <iostream>
#include <chrono>
#include <random>


/**
 * @brief This class is a helper to generate random Metadata objects.
 * It is initialized with a reference to a StringGenerator to generate the keys
 * in the constructor and the values in its generate function. The keys will always
 * be the same across all generated objects. The values however will be generated
 * randomly for each object. This allows the generated metadata to follow a schema
 * that the MetadataGenerator can provide.
 */
class MetadataGenerator {

    using json = nlohmann::json;

    StringGenerator& m_strGenerator;
    size_t           m_minValSize;
    size_t           m_maxValSize;
    json             m_schema    = json::object();
    json             m_prototype = json::object();

    public:

    MetadataGenerator(StringGenerator& strGen,
                      size_t numFields,
                      size_t minKeySize,
                      size_t maxKeySize,
                      size_t minValSize,
                      size_t maxValSize)
    : m_strGenerator(strGen)
    , m_minValSize(minValSize)
    , m_maxValSize(maxValSize) {
        m_schema["type"] = "object";
        m_schema["properties"] = json::object();
        auto& properties = m_schema["properties"];
        for(size_t i = 0; i < numFields; ++i) {
            auto key = m_strGenerator.generate(minKeySize, maxKeySize);
            properties[key] = json::object();
            properties[key]["type"] = "string";
            m_prototype[key] = json();
        }
    }

    mofka::Metadata generate() {
        auto metadata = m_prototype;
        for(auto& p : metadata.items()) {
            p.value() = m_strGenerator.generate(m_minValSize, m_maxValSize);
        }
        return metadata;
    }

    const json& schema() const {
        return m_schema;
    }
};

#endif
