/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_METADATA_IMPL_H
#define MOFKA_METADATA_IMPL_H

#include <mofka/Json.hpp>

namespace mofka {

class MetadataImpl {

    public:

    MetadataImpl(rapidjson::Document doc)
    : m_json(std::move(doc)) {}

    rapidjson::Document m_json;
};

}

#endif
