/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "RapidJsonUtil.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Exception.hpp"

#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Metadata);

Metadata::Metadata(std::string json, bool validate)
: self(std::make_shared<MetadataImpl>(std::move(json), validate)) {}

Metadata::Metadata(rapidjson::Document json)
: self(std::make_unique<MetadataImpl>(std::move(json))) {}

Metadata::Metadata(const rapidjson::Value& json)
: self(std::make_unique<MetadataImpl>(json)) {}

const std::string& Metadata::string() const {
    self->ensureString();
    return self->m_string;
}

bool Metadata::isValidJson() const {
    return self->validateJson();
}

std::string& Metadata::string() {
    self->ensureString();
    self->m_type = MetadataImpl::Type::String; /* invalidate json */
    return self->m_string;
}

const rapidjson::Document& Metadata::json() const {
    self->ensureJson();
    return self->m_json;
}

rapidjson::Document& Metadata::json() {
    self->ensureJson();
    self->m_type = MetadataImpl::Type::ActualJson; /* invalidate string */
    return self->m_json;
}

}
