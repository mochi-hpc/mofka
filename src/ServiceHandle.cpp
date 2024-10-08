/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/ServiceHandle.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"

#include "JsonUtil.hpp"
#include "PimplUtil.hpp"
#include "ServiceHandleImpl.hpp"
#include "MofkaTopicHandle.hpp"
#include "MetadataImpl.hpp"

#include <bedrock/Client.hpp>

#include <fstream>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(ServiceHandle);

static inline std::pair<std::string, uint16_t> discoverMofkaServiceMaster(
        const bedrock::ServiceGroupHandle& bsgh) {
    std::string configs;
    constexpr const char* script = R"(
          $result = [];
          foreach($__config__.providers as $p) {
              if($p.type != "yokan") continue;
              foreach($p.tags as $tag) {
                  if($tag != "mofka:master") continue;
                  array_push($result, $p.provider_id);
              }
          }
          return $result;
      )";
    bsgh.queryConfig(script, &configs);
    auto doc = Metadata{configs};
    std::vector<std::pair<std::string, uint16_t>> masters;
    for(const auto& p : doc.json().items()) {
        const auto& address = p.key();
        for(const auto& provider_id : p.value()) {
            masters.push_back({address, provider_id.get<uint16_t>()});
        }
    }
    if(masters.empty())
        throw Exception{"Could not find a Yokan provider with the \"mofka:master\" tag"};
    // note: if multiple yokan databases have the mofka:master tag,
    // it is assume that they are linked together via RAFT to replicate the same content
    return masters[0];
}

ServiceHandle::ServiceHandle(const std::string& groupfile) {
    // try to infer the address from one of the members
    std::ifstream inputFile(groupfile);
    if(!inputFile.is_open()) {
        throw Exception{"Could not open group file"};
    }
    try {
        nlohmann::json content;
        inputFile >> content;
        if(!content.is_object()
        || !content.contains("members")
        || !content["members"].is_array()
        || !content["members"][0].is_object()
        || !content["members"][0].contains("address")
        || !content["members"][0]["address"].is_string())
            throw Exception{"Group file doesn't appear to be a correctly formatted Flock group file"};
        auto& address = content["members"][0]["address"].get_ref<const std::string&>();
        auto protocol = address.substr(0, address.find(':'));
        auto engine = thallium::engine{protocol, THALLIUM_SERVER_MODE};
        auto sh = ServiceHandle{groupfile, engine};
        self = std::move(sh.self);
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

ServiceHandle::ServiceHandle(const std::string& groupfile, thallium::engine engine) {
    std::unordered_set<std::string> addrSet;
    std::vector<std::string> addresses;

    std::ifstream inputFile(groupfile);
    if(!inputFile.is_open()) {
        throw Exception{"Could not open group file"};
    }

    try {
        nlohmann::json content;
        inputFile >> content;
        if(!content.is_object() || !content.contains("members") || !content["members"].is_array())
            throw Exception{"Group file doesn't appear to be a correctly formatted Flock group file"};
        auto& members = content["members"];
        if(members.empty())
            throw Exception{"No member found in provided Flock group file"};
        for(auto& member : members) {
            if(!member.is_object() || !member.contains("address"))
                throw Exception{"Group file doesn't appear to be a correctly formatted Flock group file"};
            auto c =  addrSet.size();
            auto& addr = member["address"].get_ref<const std::string&>();
            addrSet.insert(addr);
            if(c < addrSet.size())
                addresses.push_back(addr);
        }
        auto bedrock_client = bedrock::Client{engine};
        auto bsgh = bedrock_client.makeServiceGroupHandle(addresses);
        auto master = discoverMofkaServiceMaster(bsgh);
        self = std::make_shared<ServiceHandleImpl>(engine, std::move(bsgh), master);
    } catch(const std::exception& ex) {
        throw Exception(ex.what());
    }
}

size_t ServiceHandle::numServers() const {
    return self->m_bsgh.size();
}

void ServiceHandle::createTopic(
        std::string_view name,
        Validator validator,
        PartitionSelector selector,
        Serializer serializer) {
    if(name.size() > 256) throw Exception{"Topic names cannot exceed 256 characters"};
    // A topic's informations are stored in the service' master database
    // with the keys prefixed "MOFKA:GLOBAL:<name>:". The validator is
    // located at key "MOFKA:GLOBAL:<name>:validator", and respectively
    // for the selector and serializer.
    //
    // The partitions are managed in a collection named
    // "MOFKA:GLOBAL:{}:partitions. The topic is created without any partition.
    // The partitions need to be added using ServiceHandle::add*Partition().
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
    // put the keys in the database. If any already exists,
    // this call will fail with YOKAN_ERR_KEY_EXISTS.
    try {
        self->m_yk_master_db.putMulti(3,
            keysPtrs.data(), ksizes.data(),
            valuesPtrs.data(), vsizes.data(),
            YOKAN_MODE_NEW_ONLY|YOKAN_MODE_NO_RDMA);
        auto collectionName = fmt::format("MOFKA:GLOBAL:{}:partitions", name);
        self->m_yk_master_db.createCollection(collectionName.c_str());
    } catch(const yokan::Exception& ex) {
        if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
            throw Exception{"Topic already exists"};
        } else {
            throw Exception{fmt::format(
                "Could not create topic \"{}\". "
                "Yokan error: {}",
                name, ex.what())};
        }
    }
}

TopicHandle ServiceHandle::openTopic(std::string_view name) {
    // craft the keys for the topic's validator, selector and serializer
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
    // get the length of these keys. These keys are never overwritten
    // so the length is not going to change by the time we call getMulti.
    try {
        self->m_yk_master_db.lengthMulti(3,
            keysPtrs.data(), ksizes.data(), vsizes.data(), YOKAN_MODE_NO_RDMA);
    } catch(const yokan::Exception& ex) {
        throw Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Yokan lengthMulti error: {}",
            name, ex.what())};
    } catch(const std::exception& ex) {
        throw Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Unexpected error from lengthMulti: {}",
            name, ex.what())};
    }
    // if any of the keys is not found, this is a problem
    for(size_t i = 0; i < vsizes.size(); ++i) {
        if(vsizes[i] == YOKAN_KEY_NOT_FOUND) {
            throw Exception{
                fmt::format(
                    "Topic \"{}\" not found in master database "
                    "(key {} not found)", name, keys[i])};
        }
    }

    // get the values for the keys
    std::array<std::string, 3> values;
    for(size_t i = 0; i < 3; ++i) {
        values[i].resize(vsizes[i]);
    }
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
    } catch(const std::exception& ex) {
        throw Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Unexpected error from getMulti: {}",
            name, ex.what())};
    }

    // deserialize the validator, selector, and serializer from their Metadata
    auto validator = Validator::FromMetadata(Metadata{values[0]});
    auto selector = PartitionSelector::FromMetadata(Metadata{values[1]});
    auto serializer = Serializer::FromMetadata(Metadata{values[2]});

    // create a Collection object to access the collection of partitions
    auto partitionCollection = yokan::Collection{
        fmt::format("MOFKA:GLOBAL:{}:partitions", name).c_str(),
        self->m_yk_master_db};
    std::vector<Metadata> partitionsMetadata;
    size_t oldsize;
    yk_id_t startID = 0;

    // read the partitions from the collection
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

    // Deserialize the each partition information from its Metadata.
    // They should have a uuid field, an address field, and
    // a provider_id field.
    std::vector<SP<MofkaPartitionInfo>> partitionsList;
    for(auto& partitionMetadata : partitionsMetadata) {
        const auto& partitionMetadataJson = partitionMetadata.json();
        auto uuid = UUID::from_string(
                partitionMetadataJson["uuid"].get_ref<const std::string&>().c_str());
        const auto& address = partitionMetadataJson["address"].get_ref<const std::string&>();
        uint16_t provider_id = partitionMetadataJson["provider_id"].get<uint16_t>();
        auto partitionInfo = std::make_shared<MofkaPartitionInfo>(
            uuid, thallium::provider_handle{
                self->m_engine.lookup(address),
                provider_id}
        );
        partitionsList.push_back(std::move(partitionInfo));
    }

    return TopicHandle{std::make_shared<MofkaTopicHandle>(
        self->m_engine, name,
        std::move(validator),
        std::move(selector),
        std::move(serializer),
        std::move(partitionsList))};
}

void ServiceHandle::addMemoryPartition(std::string_view topic_name,
                                       size_t server_rank,
                                       std::string_view pool_name) {
    addCustomPartition(topic_name, server_rank, "memory", {}, {}, pool_name);
}

void ServiceHandle::addDefaultPartition(std::string_view topic_name,
                                        size_t server_rank,
                                        std::string_view metadata_provider,
                                        std::string_view data_provider,
                                        const Metadata& config,
                                        std::string_view pool_name) {
    PartitionDependencies dependencies = {
        {"metadata", {std::string{metadata_provider.data(), metadata_provider.size()}}},
        {"data", {std::string{data_provider.data(), data_provider.size()}}}
    };
    if(metadata_provider.size() == 0 || data_provider.size() == 0) {
        try {
            auto server = self->m_bsgh[server_rank];
            auto get_candidate_providers_script = R"(
            $result = {};
            $result["address"] = $__config__.margo.mercury.address;
            foreach($__config__.providers as $p) {
                if($p.type != "yokan") continue;
                if(!is_array($p.tags)) continue;
                if(in_array("mofka:metadata", $p.tags)) {
                    $result["metadata"] = $p.name;
                    break;
                }
            }
            foreach($__config__.providers as $p) {
                if($p.type != "warabi") continue;
                if(!is_array($p.tags)) continue;
                if(in_array("mofka:data", $p.tags)) {
                    $result["data"] = $p.name;
                    break;
                }
            }
            return $result;
            )";
            std::string script_result;
            server.queryConfig(get_candidate_providers_script, &script_result);

            auto candidates = nlohmann::json::parse(script_result);

            if(metadata_provider.empty()) {
                if(candidates.contains("metadata")) {
                    dependencies["metadata"] = {
                        candidates["metadata"].get<std::string>() + "@"
                        + candidates["address"].get<std::string>()};
                } else {
                    throw Exception(
                        "No metadata provider provided or found in server. "
                        "Please provide one or make sure a Yokan provider exists "
                        "with the tag \"mofka:metadata\" in the server.");
                }
            }
            if(data_provider.empty()) {
                if(candidates.contains("data")) {
                    dependencies["data"] = {
                        candidates["data"].get<std::string>() + "@"
                        + candidates["address"].get<std::string>()};
                } else {
                    throw Exception(
                        "No data provider provided or found in server. "
                        "Please provide one or make sure a Warabi provider exists "
                        "with the tag \"mofka:data\" in the server.");
                }
            }

        } catch(const bedrock::Exception& ex) {
            throw Exception{
                fmt::format("Error when querying server configuration: {}", ex.what())
            };
        }
    }

    addCustomPartition(topic_name, server_rank, "default", config, dependencies, pool_name);
}

void ServiceHandle::addCustomPartition(
    std::string_view topic_name,
    size_t server_rank,
    std::string_view partition_type,
    const Metadata& partition_config,
    const PartitionDependencies& dependencies,
    std::string_view pool_name) {

    auto partition_uuid = UUID::generate();
    auto provider_name  = fmt::format("{}_partition_{}", topic_name, partition_uuid.to_string().substr(0, 8));
    uint16_t provider_id;

    // spin up the provider in the server
    std::string provider_address;
    try {
        auto server = self->m_bsgh[server_rank];
        auto get_address_script = R"(
            return $__config__.margo.mercury.address;
        )";
        server.queryConfig(get_address_script, &provider_address);
        auto provider_desciption = nlohmann::json::object();
        provider_desciption["name"]   = provider_name;
        provider_desciption["type"]   = "mofka";
        provider_desciption["pool"]   = pool_name.size() ? pool_name : "__primary__";
        provider_desciption["config"] = nlohmann::json::object();
        provider_desciption["config"]["topic"]     = topic_name;
        provider_desciption["config"]["type"]      = partition_type;
        provider_desciption["config"]["uuid"]      = partition_uuid.to_string();
        provider_desciption["config"]["partition"] = nlohmann::json::parse(partition_config.string());
        provider_desciption["tags"] = nlohmann::json::array();
        provider_desciption["tags"].push_back("morka:partition");
        provider_desciption["dependencies"] = nlohmann::json::object();
        for(auto& p : dependencies) {
            provider_desciption["dependencies"][p.first] = nlohmann::json::array();
            for(auto& dep : p.second)
                provider_desciption["dependencies"][p.first].push_back(dep);
        }

        server.addProvider(provider_desciption.dump(), &provider_id);
    } catch(const bedrock::Exception& ex) {
        throw Exception{
            fmt::format(
                "Could not create partition for topic \"{}\". "
                "Bedrock error: {}", topic_name, ex.what())
        };
    }

    // add the information about the provider in the master database
    auto partitionCollection = yokan::Collection{
        fmt::format("MOFKA:GLOBAL:{}:partitions", topic_name).c_str(),
        self->m_yk_master_db};
    try {
        auto partition_info = fmt::format(
            "{{\"address\":\"{}\",\"provider_id\":{},\"uuid\":\"{}\"}}",
            provider_address, provider_id, partition_uuid.to_string()
        );
        partitionCollection.store(partition_info.c_str(), partition_info.size());
    } catch(yokan::Exception& ex) {
        throw Exception{
            fmt::format(
                "Could not add partition info to master database for topic \"{}\"."
                "Yokan error: {}", topic_name, ex.what())
        };
    }
}

}
