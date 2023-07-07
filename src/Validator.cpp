/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/Validator.hpp"
#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"
#include "SimpleValidatorImpl.hpp"

namespace mofka {

using ValidatorImpl = ValidatorInterface;

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Validator);

Validator::Validator()
: self(std::make_shared<SimpleValidatorImpl>()) {}

void Validator::validate(const Metadata& metadata) const {
    self->validate(metadata);
}

Metadata Validator::metadata() const {
    return self->metadata();
}

void Validator::fromMetadata(const Metadata& metadata) {
    self->fromMetadata(metadata);
}

}
