/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_YOKAN_EVENT_STORE_HPP
#define MOFKA_YOKAN_EVENT_STORE_HPP

#include "RapidJsonUtil.hpp"
#include <yokan/cxx/collection.hpp>
#include <mofka/ConsumerHandle.hpp>
#include <mofka/BatchSize.hpp>
#include <mofka/UUID.hpp>
#include <mofka/EventID.hpp>
#include <mofka/Result.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataDescriptor.hpp>
#include <mofka/BulkRef.hpp>
#include <mofka/BufferWrapperArchive.hpp>
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
        std::vector<yk_id_t> ids(count);

        try {
            m_metadata_coll.storeBulk(
                count,
                remoteBulk.handle.get_bulk(),
                remoteBulk.offset,
                remoteBulk.size,
                ids.data(),
                remoteBulk.address.c_str(),
                YOKAN_MODE_DEFAULT);
        } catch(const yokan::Exception& ex) {
            result.success() = false;
            result.error() = fmt::format(
                "Yokan Collection::storeBulk failed: {}",
                ex.what());
        }

        result.value() = ids[0];

        return result;
    }

    Result<void> storeDataDescriptors(
        EventID firstID,
        const std::vector<DataDescriptor>& descriptors) {

        Result<void> result;

        const auto count = descriptors.size();
        std::vector<size_t> descriptorSizes(count);
        std::vector<yk_id_t> ids(count);
        for(size_t i = 0; i < count; ++i) ids[i] = firstID + i;

        std::vector<char> serializedDescriptors;
        BufferWrapperOutputArchive outputArchive{serializedDescriptors};

        size_t offset = 0;
        for(size_t i = 0; i < count; ++i) {
            auto& descriptor = descriptors[i];
            descriptor.save(outputArchive);
            descriptorSizes[i] = serializedDescriptors.size() - offset;
            offset = serializedDescriptors.size();
        }

        try {
            m_descriptors_coll.updatePacked(
                count, ids.data(),
                serializedDescriptors.data(),
                descriptorSizes.data(),
                YOKAN_MODE_UPDATE_NEW);
        } catch(const yokan::Exception& ex) {
            result.success() = false;
            result.error() = fmt::format(
                "Yokan Collection::updatePacked failed: {}",
                ex.what());
        }

        return result;
    }

    Result<void> feed(
            ConsumerHandle consumerHandle,
            EventID firstID,
            BatchSize batchSize) {
        // TODO
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
