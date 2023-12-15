/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "RapidJsonUtil.hpp"
#include "mofka/ServiceHandle.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"

#include "PimplUtil.hpp"
#include "ClientImpl.hpp"
#include "ServiceHandleImpl.hpp"
#include "TopicHandleImpl.hpp"
#include "MetadataImpl.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(ServiceHandle);

Client ServiceHandle::client() const {
    return Client(self->m_client);
}

void ServiceHandle::createTopic(
        std::string_view name,
        Validator validator,
        PartitionSelector selector,
        Serializer serializer) {
    if(name.size() > 256) throw Exception{"Topic names cannot exceed 256 characters"};
    // A topic's informations are stored in the service' master database
    // the key "MOFKA:GLOBAL:<name>:". The validator is located at key
    // "MOFKA:GLOBAL:<name>:validator", and respectively for the selector
    // and serializer.
    //
    // The partitions are managed in a collection named
    // "MOFKA:GLOBAL:{}:partitions.
    std::array<std::string, 3> keys = {
        fmt::format("MOFKA:GLOBAL:{}:validator",  name),
        fmt::format("MOFKA:GLOBAL:{}:selector",   name),
        fmt::format("MOFKA:GLOBAL:{}:serializer", name),
    };
    std::array<const void*, 3> keysPtrs = {
        keys[0].data(), keys[1].data(), keys[2].data()
    };
    std::array<size_t, 3> ksizes = {
        keys[0].size(), keys[1].size(), keys[2].size()
    };
    std::array<Metadata, 3> values = {
        validator.metadata(),
        selector.metadata(),
        serializer.metadata()
    };
    std::array<const void*, 3> valuesPtrs = {
        values[0].string().c_str(),
        values[1].string().c_str(),
        values[2].string().c_str()
    };
    std::array<size_t, 3> vsizes = {
        values[0].string().size(),
        values[1].string().size(),
        values[2].string().size()
    };
    try {
        self->m_yk_master_db.putMulti(3,
            keysPtrs.data(), ksizes.data(),
            valuesPtrs.data(), vsizes.data(),
            YOKAN_MODE_NEW_ONLY|YOKAN_MODE_NO_RDMA);
        auto partitionsCollection = fmt::format("MOFKA:GLOBAL:{}:partitions", name);
        self->m_yk_master_db.createCollection(partitionsCollection.c_str());
    } catch(const yokan::Exception& ex) {
        if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
            throw Exception{"Topic already exists"};
        } else {
            throw Exception{fmt::format(
                "Could not create topic \"{}\". "
                "Yokan putMulti error: {}",
                name, ex.what())};
        }
    }
}

TopicHandle ServiceHandle::openTopic(std::string_view name) {
    // craft the keys for the topic
    std::array<std::string, 3> keys = {
        fmt::format("MOFKA:GLOBAL:{}:validator",  name),
        fmt::format("MOFKA:GLOBAL:{}:selector",   name),
        fmt::format("MOFKA:GLOBAL:{}:serializer", name),
    };
    std::array<const void*, 3> keysPtrs = {
        keys[0].data(), keys[1].data(), keys[2].data()
    };
    std::array<size_t, 3> ksizes = {
        keys[0].size(), keys[1].size(), keys[2].size()
    };
    std::array<size_t, 3> vsizes = {0, 0, 0};
    try {
        self->m_yk_master_db.lengthMulti(3,
            keysPtrs.data(), ksizes.data(), vsizes.data(), YOKAN_MODE_NO_RDMA);
    } catch(const yokan::Exception& ex) {
        throw Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Yokan lengthMulti error: {}",
            name, ex.what())};
    }
    if(std::find(ksizes.begin(), ksizes.end(), YOKAN_KEY_NOT_FOUND) != ksizes.end()) {
        throw Exception{fmt::format("Topic \"{}\" not found in master database", name)};
    }

    std::array<std::string, 3> values;
    for(size_t i = 0; i < 3; ++i) values[i].resize(vsizes[i]);
    std::array<void*, 3> valuesPtrs = {
        values[0].data(), values[1].data(), values[2].data()
    };
    try {
        self->m_yk_master_db.getMulti(3,
            keysPtrs.data(), ksizes.data(),
            valuesPtrs.data(), vsizes.data(), YOKAN_MODE_NO_RDMA);
    } catch(const yokan::Exception& ex) {
        throw Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Yokan getMulti error: {}",
            name, ex.what())};
    }

    auto validator = Validator::FromMetadata(Metadata{values[0]});
    auto selector = PartitionSelector::FromMetadata(Metadata{values[1]});
    auto serializer = Serializer::FromMetadata(Metadata{values[2]});

    auto partitionCollection = yokan::Collection{
        fmt::format("MOFKA:GLOBAL:{}:partitions", name).c_str(),
        self->m_yk_master_db};
    std::vector<Metadata> partitionsMetadata;
    size_t oldsize;
    yk_id_t startID = 0;
    try {
        do {
            oldsize = partitionsMetadata.size();
            partitionCollection.iter(startID, nullptr, 0, 32,
                [&](size_t, yk_id_t id, const void* value, size_t vsize) mutable -> yk_return_t {
                    startID = id + 1;
                    auto partitionMetadata = Metadata{
                        std::string{static_cast<const char*>(value), vsize}
                    };
                    partitionsMetadata.push_back(std::move(partitionMetadata));
                    return YOKAN_SUCCESS;
                });
        } while(oldsize != partitionsMetadata.size());
    } catch(const yokan::Exception& ex) {
        throw Exception{fmt::format(
            "Could not lookup partitions for topic \"{}\". Yokan iter error: {}",
            name, ex.what())};
    }

    std::vector<PartitionInfo> partitionsList;
    for(auto& partitionMetadata : partitionsMetadata) {
        const auto& partitionMetadataJson = partitionMetadata.json();
        auto uuid = UUID::from_string(
                partitionMetadataJson["__uuid__"].GetString());
        auto address = partitionMetadataJson["__address__"].GetString();
        uint16_t provider_id = partitionMetadataJson["__provider_id__"].GetUint();
        auto partitionInfo = std::make_shared<PartitionInfoImpl>(
            uuid, thallium::provider_handle{
                self->m_client->m_engine.lookup(address),
                provider_id}
        );
        partitionsList.push_back(partitionInfo);
    }

    return std::make_shared<TopicHandleImpl>(
        name, self,
        std::move(validator),
        std::move(selector),
        std::move(serializer),
        std::move(partitionsList));
}

}
