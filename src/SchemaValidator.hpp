/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SCHEMA_VALIDATOR_H
#define MOFKA_SCHEMA_VALIDATOR_H

#include "JsonUtil.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Validator.hpp"
#include "mofka/Json.hpp"
#include "MetadataImpl.hpp"

namespace mofka {

class SchemaValidator : public ValidatorInterface {

    using json = nlohmann::json;

    json                m_json_schema;
    JsonSchemaValidator m_json_validator;

    public:

    SchemaValidator(json schema)
    : m_json_schema(std::move(schema))
    , m_json_validator(m_json_schema) {}

    void validate(const Metadata& metadata, const Data& data) const override {
        (void)data;
        auto errors = m_json_validator.validate(metadata.json());
        if(!errors.empty()) {
            std::stringstream ss;
            ss << "Metadata does not comply to required schema:\n";
            for(size_t i=0; i < errors.size()-1; ++i) {
                ss << errors[i];
            }
            ss << errors[errors.size()-1];
            throw InvalidMetadata{ss.str()};
        }
    }

    Metadata metadata() const override {
        auto config = json::object();
        config["type"] = "schema";
        config["schema"] = m_json_schema;
        return Metadata{std::move(config)};
    }

    static std::unique_ptr<ValidatorInterface> create(const Metadata& metadata) {
        if(!metadata.isValidJson())
            throw Exception{"Provided Metadata is not valid JSON"};
        if(!metadata.json().contains("schema"))
            throw Exception{"SchemaValidator is expecting a \"schema\" entry in its configuration"};
        return std::make_unique<SchemaValidator>(metadata.json()["schema"]);
    }

};

}

#endif
