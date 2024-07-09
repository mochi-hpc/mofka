/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENTBRIDGE_VALIDATOR_H
#define MOFKA_EVENTBRIDGE_VALIDATOR_H

#include "JsonUtil.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Validator.hpp"
#include "mofka/Json.hpp"
#include "MetadataImpl.hpp"

namespace mofka {

class EventbridgeValidator : public ValidatorInterface {

    using json = nlohmann::json;

    json                             m_schema;
    std::function<bool(const json&)> m_validator;

    public:

    EventbridgeValidator(json schema, std::function<bool(const json&)> func)
    : m_schema(std::move(schema))
    , m_validator(std::move(func)) {}

    void validate(const Metadata& metadata, const Data& data) const override;

    Metadata metadata() const override;

    static std::unique_ptr<ValidatorInterface> create(const Metadata& metadata);

};

}

#endif
