/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DEFAULT_VALIDATOR_H
#define MOFKA_DEFAULT_VALIDATOR_H

#include "JsonUtil.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Validator.hpp"
#include "mofka/Json.hpp"
#include "MetadataImpl.hpp"

namespace mofka {

class DefaultValidator : public ValidatorInterface {

    using json = nlohmann::json;

    public:

    void validate(const Metadata& metadata, const Data& data) const override {
        (void)data;
        if(!metadata.isValidJson())
            throw InvalidMetadata("Metadata object does not contain valid JSON metadata");
    }

    Metadata metadata() const override {
        return Metadata{"{\"type\":\"default\"}"_json};
    }

    static std::unique_ptr<ValidatorInterface> create(const Metadata& metadata) {
        (void)metadata;
        return std::make_unique<DefaultValidator>();
    }

};

}

#endif
