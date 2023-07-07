/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SIMPLE_VALIDATOR_IMPL_H
#define MOFKA_SIMPLE_VALIDATOR_IMPL_H

#include "mofka/Metadata.hpp"
#include "mofka/Validator.hpp"
#include "mofka/Json.hpp"
#include "MetadataImpl.hpp"
#include "RapidJsonUtil.hpp"

namespace mofka {

class SimpleValidatorImpl : public ValidatorInterface {

    public:

    void validate(const Metadata& metadata) const override {
        if(!metadata.isValidJson())
            throw InvalidMetadata("Metadata object does not contain valid JSON metadata");
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
