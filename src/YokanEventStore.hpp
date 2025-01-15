/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_YOKAN_EVENT_STORE_HPP
#define MOFKA_YOKAN_EVENT_STORE_HPP

#include "JsonUtil.hpp"
#include <yokan/cxx/client.hpp>
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
#include <numeric>

namespace mofka {

class YokanEventStore {

    thallium::engine             m_engine;
    std::string                  m_topic_name;
    yokan::Client                m_yokan_client;
    yokan::Database              m_database;
    yokan::Collection            m_metadata_coll;
    yokan::Collection            m_descriptors_coll;
    size_t                       m_num_events = 0;
    thallium::mutex              m_num_events_mtx;
    thallium::condition_variable m_num_events_cv;
    std::atomic<bool>            m_is_marked_complete = false;

    public:

    void wakeUp() {
        m_num_events_cv.notify_all();
    };

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

        result.value() = ids[0] - 1; // IDs start at 1 in the underlying DB
        {
            auto g = std::unique_lock{m_num_events_mtx};
            m_num_events += count;
        }
        m_num_events_cv.notify_all();

        return result;
    }

    Result<void> storeDataDescriptors(
        EventID firstID,
        const std::vector<DataDescriptor>& descriptors) {

        Result<void> result;

        const auto count = descriptors.size();
        std::vector<size_t> descriptorSizes(count);
        std::vector<char> serializedDescriptors;
        BufferWrapperOutputArchive outputArchive{serializedDescriptors};

        size_t offset = 0;
        for(size_t i = 0; i < count; ++i) {
            auto& descriptor = descriptors[i];
            if(descriptor.size() > 0) {
                descriptor.save(outputArchive);
                descriptorSizes[i] = serializedDescriptors.size() - offset;
                offset = serializedDescriptors.size();
            } else {
                descriptorSizes[i] = 0;
            }
        }

        if(offset > 0) {

            std::vector<yk_id_t> ids(count);
            for(size_t i = 0; i < count; ++i) ids[i] = firstID + 1 + i;

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
        }

        return result;
    }

    Result<void> markAsComplete() {
        Result<void> result;
        result.success() = true;
        m_metadata_coll.update(0, "T", 1);
        m_is_marked_complete = true;
        m_num_events_cv.notify_all();
        return result;
    }

    Result<void> feed(
            ConsumerHandle consumerHandle,
            EventID firstID,
            BatchSize batchSize) {

        Result<void> result;

        if(batchSize.value == 0 || batchSize == BatchSize::Adaptive())
            batchSize = BatchSize{32};

        auto c = batchSize.value;

        struct BufferSet {

            std::vector<yk_id_t> ids;
            std::vector<size_t>  metadata_sizes;
            std::vector<char>    metadata_buffer;

            // buffers to hold the metadata and descriptors
            // note: because we are using docLoad for descriptors, we need
            // the sizes and documents to be contiguous even if the number
            // of items requested varies.
            std::vector<char> descriptors_sizes_and_data;

            thallium::bulk local_metadata_bulk;
            thallium::bulk local_descriptors_bulk;

            std::string self_addr;

            BulkRef metadata_sizes_bulk_ref;
            BulkRef metadata_bulk_ref;
            BulkRef descriptors_sizes_bulk_ref;
            BulkRef descriptors_bulk_ref;

            BufferSet(thallium::engine engine, size_t count)
            : ids(count)
            , metadata_sizes(count)
            , metadata_buffer(count * 1024 * 8)
            , descriptors_sizes_and_data(count * (sizeof(size_t) + 1024))
            , self_addr(engine.self())
            {

                // expose these buffers as bulk handles
                local_metadata_bulk = engine.expose(
                    {{metadata_sizes.data(), count*sizeof(metadata_sizes[0])},
                    {ids.data(), count*sizeof(ids[0])},
                    {metadata_buffer.data(), metadata_buffer.size()*sizeof(metadata_buffer[0])}},
                    thallium::bulk_mode::read_write);
                local_descriptors_bulk = engine.expose(
                    {{descriptors_sizes_and_data.data(),
                      descriptors_sizes_and_data.size()}},
                    thallium::bulk_mode::read_write);
                // create bulk refs
                metadata_sizes_bulk_ref = BulkRef{
                    local_metadata_bulk,
                    0,
                    count*sizeof(size_t),
                    self_addr
                };
                metadata_bulk_ref = BulkRef{
                    local_metadata_bulk,
                    count*(sizeof(size_t) + sizeof(yk_id_t)),
                    metadata_buffer.size(),
                    self_addr
                };
                descriptors_sizes_bulk_ref = BulkRef{
                    local_descriptors_bulk,
                    0,
                    count*sizeof(size_t),
                    self_addr
                };
                descriptors_bulk_ref = BulkRef{
                    local_descriptors_bulk,
                    count*sizeof(size_t),
                    descriptors_sizes_and_data.size() - count*sizeof(size_t),
                    self_addr
                };

            }

        };

        auto b1 = std::make_shared<BufferSet>(m_engine, c);
        auto b2 = std::make_shared<BufferSet>(m_engine, c);
        Future<void> lastFeed;

        while(!consumerHandle.shouldStop()) {

            bool should_stop = false;
            size_t num_available_events = 0;
            while(true) {
                auto g = std::unique_lock{m_num_events_mtx};
                // find the number of events we can send
                num_available_events = m_num_events - firstID;
                should_stop = consumerHandle.shouldStop();
                if(num_available_events > 0 || should_stop || m_is_marked_complete) break;
                m_num_events_cv.wait(g);
            }
            if(should_stop) break;

            if(num_available_events == 0) { // m_is_marked_complete must be true
                                            // feed consumer 0 events with first_id = NoMoreEvents to indicate
                                            // that there are no more events to consume from this partition
                if(lastFeed) lastFeed.wait();
                lastFeed = consumerHandle.feed(
                        0, NoMoreEvents, BulkRef{}, BulkRef{}, BulkRef{}, BulkRef{});
                break;
            }

            // list metadata documents
            m_metadata_coll.listBulk(
                    firstID+1, 0, b1->local_metadata_bulk.get_bulk(),
                    0, b1->metadata_buffer.size(), true, batchSize.value);

            // check how many we actually pulled
            auto it = std::find_if(b1->metadata_sizes.begin(),
                                   b1->metadata_sizes.end(),
                                   [](auto size) {
                                        return size > YOKAN_LAST_VALID_SIZE;
                                   });

            size_t num_events = it - b1->metadata_sizes.begin();
            b1->metadata_bulk_ref.size = std::accumulate(b1->metadata_sizes.begin(), it, (size_t)0);
            b1->metadata_sizes_bulk_ref.size = num_events*sizeof(size_t);

            // load the corresponding descriptors
            m_descriptors_coll.loadBulk(
                    num_events, b1->ids.data(), b1->local_descriptors_bulk.get_bulk(),
                    0, b1->local_descriptors_bulk.size(), true);
            auto descriptors_sizes = reinterpret_cast<size_t*>(b1->descriptors_sizes_and_data.data());
            b1->descriptors_sizes_bulk_ref.size = num_events*sizeof(size_t);
            for(size_t i = 0; i < num_events; ++i) {
                if(descriptors_sizes[i] > YOKAN_LAST_VALID_SIZE)
                    descriptors_sizes[i] = 0;
            }
            b1->descriptors_bulk_ref.offset = b1->descriptors_sizes_bulk_ref.size;
            b1->descriptors_bulk_ref.size = std::accumulate(
                descriptors_sizes, descriptors_sizes + num_events, (size_t)0);

            if(lastFeed)
                lastFeed.wait();

            // feed the consumer handle
            lastFeed = consumerHandle.feed(
                    num_events, firstID,
                    b1->metadata_sizes_bulk_ref,
                    b1->metadata_bulk_ref,
                    b1->descriptors_sizes_bulk_ref,
                    b1->descriptors_bulk_ref);

            firstID += num_events;

            std::swap(b1, b2);
        }

        if(lastFeed)
            lastFeed.wait();

        return result;
    }

    YokanEventStore(
        thallium::engine engine,
        std::string topic_name,
        yokan::Client yokan_client,
        yokan::Database db,
        yokan::Collection metadata_coll,
        yokan::Collection descriptors_coll,
        size_t num_events,
        bool marked_as_complete)
    : m_engine(std::move(engine))
    , m_topic_name(std::move(topic_name))
    , m_yokan_client(std::move(yokan_client))
    , m_database(std::move(db))
    , m_metadata_coll(std::move(metadata_coll))
    , m_descriptors_coll(std::move(descriptors_coll))
    , m_num_events(num_events)
    , m_is_marked_complete{marked_as_complete} {}

    static std::unique_ptr<YokanEventStore> create(
            thallium::engine engine,
            const std::string& topic_name,
            const UUID& partition_uuid,
            thallium::provider_handle yokan_ph) {

        auto yokan_client = yokan::Client{engine};
        auto database = yokan_client.makeDatabaseHandle(yokan_ph.get_addr(), yokan_ph.provider_id());

        auto metadataCollName = topic_name + "--" + partition_uuid.to_string() + "--md";
        auto descriptorsCollName = topic_name + "--" + partition_uuid.to_string() + "--dd";
        bool init_collection= false;
        if(!database.collectionExists(metadataCollName.c_str())) {
            database.createCollection(metadataCollName.c_str());
            init_collection = true;
        }
        if(!database.collectionExists(descriptorsCollName.c_str())) {
            database.createCollection(descriptorsCollName.c_str());
        }
        auto metadata_coll = yokan::Collection{metadataCollName.c_str(), database};
        auto descriptors_coll = yokan::Collection{descriptorsCollName.c_str(), database};
        auto marked_as_complete = false;
        if(init_collection) {
            metadata_coll.store("F", 1);    // marked as completed
            descriptors_coll.store("F", 1); // marked as completed
        } else {
            std::string tmp(1, '\0');
            size_t s = 1;
            metadata_coll.load(0, tmp.data(), &s);
            if(tmp == "T") marked_as_complete = true;
        }
        auto num_events = metadata_coll.size() - 1;
        return std::make_unique<YokanEventStore>(
            std::move(engine),
            std::move(topic_name),
            std::move(yokan_client),
            std::move(database),
            std::move(metadata_coll),
            std::move(descriptors_coll),
            num_events,
            marked_as_complete);
    }

};

} // namespace mofka

#endif
