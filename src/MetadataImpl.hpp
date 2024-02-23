/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_METADATA_IMPL_H
#define MOFKA_METADATA_IMPL_H

#include <iostream>
#include "JsonUtil.hpp"
#include "mofka/Json.hpp"
#include "mofka/Metadata.hpp"
#include <fmt/format.h>

namespace mofka {

class MetadataImpl {

    using json = nlohmann::json;

    public:

    enum Type : uint8_t {
        String     = 0x1, /* string field is up to date */
        ValidJson  = 0x3, /* the string is up to date and we know it's valid JSON (implies String) */
        ActualJson = 0x6, /* the json field is up to date (implies ValidJson) */
    };

    explicit MetadataImpl(json doc)
    : m_json(std::move(doc))
    , m_type(Type::ActualJson) {}

    MetadataImpl(std::string str, bool validate)
    : m_string(std::move(str))
    , m_type(Type::String) {
        if(validate && !ValidateIsJson(m_string))
            throw Exception("String provided to Metadata constructor is not valid JSON");
        if(validate)
            m_type = Type::ValidJson;
    }

    void ensureString() {
        if(m_type & Type::String) return;
        m_string = m_json.dump();
        m_type |= Type::String;
    }

    void ensureJson() {
        if((m_type & Type::ActualJson) == Type::ActualJson) return;
        try {
            m_json = json::parse(m_string);
        } catch(const std::exception& ex) {
           throw Exception(fmt::format(
                "Could not parse Metadata string: {}", ex.what()));
        }
        m_type |= Type::ActualJson;
    }

    bool validateJson() {
        if(m_type & Type::ValidJson) return true;
        if(ValidateIsJson(m_string)) {
            m_type |= Type::ValidJson;
            return true;
        } else {
            return false;
        }
    }

    std::string m_string;
    json        m_json;
    uint8_t     m_type;
};

template<typename A>
void save(A& ar, const Metadata& metadata) {
    ar((const std::string&)metadata.string());
}

template<typename A>
void load(A& ar, Metadata& metadata) {
    std::string str;
    ar(str);
    metadata.string() = std::move(str);
}

}

#endif
