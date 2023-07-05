/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Metadata.hpp"
#include "mofka/Exception.hpp"

#include "MetadataImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Metadata);

Metadata Metadata::FromJSON(std::string_view json) {
    rapidjson::Document doc;
    doc.Parse(json.data(), json.size());
    return std::make_shared<MetadataImpl>(std::move(doc));
}

Metadata Metadata::FromJSON(rapidjson::Document doc) {
    return std::make_shared<MetadataImpl>(std::move(doc));
}

}
