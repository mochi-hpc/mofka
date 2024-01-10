/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_YOKAN_EVENT_STORE_HPP
#define MOFKA_YOKAN_EVENT_STORE_HPP

#include "RapidJsonUtil.hpp"
#include <yokan/cxx/collection.hpp>
#include <mofka/UUID.hpp>
#include <mofka/EventID.hpp>
#include <mofka/Result.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataDescriptor.hpp>
#include <mofka/BulkRef.hpp>
#include <spdlog/spdlog.h>
#include <cstddef>
#include <string_view>
#include <unordered_map>

namespace mofka {

class YokanEventStore {

    thallium::engine  m_engine;
    yokan::Database   m_database;
    yokan::Collection m_metadata_coll;
    yokan::Collection m_descriptors_coll;

    public:

    Result<EventID> appendMetadata(
            size_t count,
            const BulkRef& remoteBulk) {

        Result<EventID> result;

        /* lookup the sender. */
        const auto source = m_engine.lookup(remoteBulk.address);

        return result;
    }

    Result<void> storeDataDescriptors(
        EventID firstID,
        const std::vector<DataDescriptor>& descriptors) {

    }

    YokanEventStore(
        thallium::engine engine,
        yokan::Database db,
        yokan::Collection metadata_coll,
        yokan::Collection descriptors_coll)
    : m_engine(std::move(engine))
    , m_database(std::move(db))
    , m_metadata_coll(std::move(metadata_coll))
    , m_descriptors_coll(std::move(descriptors_coll)) {}

    static std::unique_ptr<YokanEventStore> create(
            thallium::engine engine,
            const std::string& topic_name,
            const UUID& partition_uuid,
            yk_database_handle_t db) {
        auto database = yokan::Database{db};
        auto metadataCollName = topic_name + "/" + partition_uuid.to_string() + "/md";
        auto descriptorsCollName = topic_name + "/" + partition_uuid.to_string() + "/dd";
        if(!database.collectionExists(metadataCollName.c_str())) {
            database.createCollection(metadataCollName.c_str());
        }
        if(!database.collectionExists(descriptorsCollName.c_str())) {
            database.createCollection(descriptorsCollName.c_str());
        }
        auto metadata_coll = yokan::Collection{metadataCollName.c_str(), database};
        auto descriptors_coll = yokan::Collection{descriptorsCollName.c_str(), database};
        return std::make_unique<YokanEventStore>(
            std::move(engine),
            std::move(database),
            std::move(metadata_coll),
            std::move(descriptors_coll));
    }

};

} // namespace mofka

#endif
