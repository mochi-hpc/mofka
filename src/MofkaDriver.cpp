/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Driver.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/TopicHandle.hpp>

#include "JsonUtil.hpp"
#include "Logging.hpp"

#include "mofka/MofkaTopicHandle.hpp"
#include "mofka/MofkaDriver.hpp"
#include "mofka/MofkaThreadPool.hpp"

#include <bedrock/Client.hpp>
#include <spdlog/spdlog.h>

#include <fstream>

namespace mofka {

DIASPORA_REGISTER_DRIVER(mofka, mofka, MofkaDriver);

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
    spdlog::trace("[mofka:client] Discovering Mofka service master database");
    bsgh.queryConfig(script, &configs);
    auto doc = diaspora::Metadata{configs};
    std::vector<std::pair<std::string, uint16_t>> masters;
    for(const auto& p : doc.json().items()) {
        const auto& address = p.key();
        for(const auto& provider_id : p.value()) {
            masters.push_back({address, provider_id.get<uint16_t>()});
        }
    }
    if(masters.empty()) {
        throw diaspora::Exception{"Could not find a Yokan provider with the \"mofka:master\" tag"};
    }
    spdlog::trace("[mofka:client] Found master database at address={} with provider_id={}",
                  masters[0].first, masters[0].second);
    // note: if multiple yokan databases have the mofka:master tag,
    // it is assume that they are linked together via RAFT to replicate the same content
    return masters[0];
}

std::shared_ptr<diaspora::DriverInterface> MofkaDriver::create(const diaspora::Metadata& options) {
    // this static variable maps a protocol to a possibly existing driver
    static std::unordered_map<std::string, std::weak_ptr<MofkaDriver>> s_drivers;
    thallium::engine engine;
    try {

        auto& config = options.json();
        if(!config.is_object())
            throw diaspora::Exception{"Options passed to MofkaDriver::create should be an object"};

        // process the group file to get the protocol and list of servers
        auto group_file_name = config.value("group_file", std::string{""});
        if(group_file_name.empty())
            throw diaspora::Exception{"\"group_file\" key not found in options"};

        std::vector<std::string> server_addresses;
        std::ifstream group_file(group_file_name);
        if(!group_file.good()) {
            throw diaspora::Exception{"Could not find or open group file"};
        }
        nlohmann::json group_file_content;
        group_file >> group_file_content;
        if(!group_file_content.is_object()
        || !group_file_content.contains("members")
        || !group_file_content["members"].is_array()) {
            throw diaspora::Exception{
                "Group file doesn't appear to be a correctly formatted Flock group file"};
        }
        auto& members = group_file_content["members"];
        if(members.empty())
            throw diaspora::Exception{"No member found in provided Flock group file"};
        for(auto& member : members) {
            if(!member.is_object() || !member.contains("address"))
                throw diaspora::Exception{
                    "Group file doesn't appear to be a correctly formatted Flock group file"};
            auto& addr = member["address"].get_ref<const std::string&>();
            if(std::find(server_addresses.begin(), server_addresses.end(), addr) == server_addresses.end())
                server_addresses.push_back(addr);
        }
        auto protocol = server_addresses[0].substr(0, server_addresses[0].find(':'));

        // get the margo configuration
        auto margo_config = config.value("margo", nlohmann::json::object());
        if(!margo_config.is_object())
            throw diaspora::Exception{"\"margo\" key should map to object"};

        // find a possible driver
        auto driver_wptr = s_drivers[protocol];
        auto driver = driver_wptr.lock();

        if(driver) {
            engine = driver->m_engine;
            if(!margo_config.empty())
                spdlog::warn(
                    "[mofka] Newly created driver will share its"
                    " progress engine with existing drivers."
                    " Some options may be ignored as a consequence.");
        } else {
            auto margo_config_str = margo_config.dump();
            engine = thallium::engine{
                protocol.c_str(), THALLIUM_SERVER_MODE, margo_config_str.c_str()};
            auto pool = thallium::xstream::self().get_main_pools(1)[0];
            MofkaThreadPool::SetDefaultPool(pool);
        }

        // create the bedrock ServiceGroupHandle
        auto bedrock_client = bedrock::Client{engine};
        auto bsgh = bedrock_client.makeServiceGroupHandle(server_addresses);
        auto master = discoverMofkaServiceMaster(bsgh);

        // create the MofkaDriver instance
        driver = std::make_shared<MofkaDriver>(std::move(engine), std::move(bsgh), std::move(master));

        // store it in the static map
        if(!driver_wptr.lock())
            s_drivers[protocol] = driver;

        return driver;

    } catch(const std::exception& ex) {
        throw diaspora::Exception{ex.what()};
    }
}

void MofkaDriver::createTopic(
        std::string_view name,
        const diaspora::Metadata& options,
        std::shared_ptr<diaspora::ValidatorInterface> validator,
        std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
        std::shared_ptr<diaspora::SerializerInterface> serializer) {
    if(name.size() > 256) throw diaspora::Exception{"Topic names cannot exceed 256 characters"};
    // check options
    int num_partitions = 0;
    std::string partition_type = "memory";
    std::string partition_pool = "";
    std::string partition_config = "{}";
    Dependencies partition_dependencies;
    if(options.json().is_object()) {
        if(options.json().contains("partitions")) {
            if(!options.json()["partitions"].is_number())
                throw diaspora::Exception{"\"partitions\" parameter in options should be an integer"};
            num_partitions = options.json()["partitions"].get<int>();
            if(num_partitions <= 0)
                throw diaspora::Exception{"\"partitions\" value in options is invalid"};
        }
        if(options.json().contains("partitions_type")) {
            if(!options.json()["partitions_type"].is_string())
                throw diaspora::Exception{"\"partitions_type\" parameter in options should be a string"};
            options.json()["partitions_type"].get_to(partition_type);
        }
        if(options.json().contains("partitions_pool")) {
            if(!options.json()["partitions_pool"].is_string())
                throw diaspora::Exception{"\"partitions_pool\" parameter in options should be a string"};
            options.json()["partitions_pool"].get_to(partition_pool);
        }
        if(options.json().contains("partitions_config"))
            partition_config = options.json()["partitions_config"].dump();
        if(options.json().contains("partitions_dependencies")) {
            auto& deps = options.json()["partitions_dependencies"];
            if(!deps.is_object())
                throw diaspora::Exception{
                    "\"partitions_dependencies\" parameter in options should be an object"};
            for(auto& p : deps.items()) {
                if(p.value().is_string()) {
                    partition_dependencies[p.key()] = {p.value().get<std::string>()};
                } else if(p.value().is_array()) {
                    for(auto& s : p.value()) {
                        if(!s.is_string()) {
                            throw diaspora::Exception{
                                "\"partitions_dependencies\" in options should only contain"
                                " strings or arrays of strings"};
                        }
                        partition_dependencies[p.key()].push_back(s.get<std::string>());
                    }
                } else {
                    throw diaspora::Exception{
                        "\"partitions_dependencies\" in options should only contain"
                        " strings or arrays of strings"};
                }
            }
        }
    }
    spdlog::trace("[mofka:client] Creating topic {}", name);
    // A topic's informations are stored in the service' master database
    // with the keys prefixed "MOFKA:GLOBAL:<name>:". The validator is
    // located at key "MOFKA:GLOBAL:<name>:validator", and respectively
    // for the selector and serializer.
    //
    // The partitions are managed in a collection named
    // "MOFKA:GLOBAL:{}:partitions. The topic is created without any partition.
    // The partitions need to be added using MofkaDriver::add*Partition().
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
    std::array<std::string, 3> values = {
        validator->metadata().json().dump(),
        selector->metadata().json().dump(),
        serializer->metadata().json().dump()};
    std::array<const void*, 3> valuesPtrs = {
        values[0].c_str(),
        values[1].c_str(),
        values[2].c_str()
    };
    std::array<size_t, 3> vsizes = {
        values[0].size(),
        values[1].size(),
        values[2].size()
    };
    // put the keys in the database. If any already exists,
    // this call will fail with YOKAN_ERR_KEY_EXISTS.
    try {
        spdlog::trace("[mofka:client] Storing topic information"
                      " (validator, selector, serializer) in master database "
                      " for topic {}", name);
        m_yk_master_db.putMulti(3,
            keysPtrs.data(), ksizes.data(),
            valuesPtrs.data(), vsizes.data(),
            YOKAN_MODE_NEW_ONLY|YOKAN_MODE_NO_RDMA);
        auto collectionName = fmt::format("MOFKA:GLOBAL:{}:partitions", name);
        m_yk_master_db.createCollection(collectionName.c_str());
        spdlog::trace("[mofka:client] Creating collection to store partitions for topic {}", name);
    } catch(const yokan::Exception& ex) {
        if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
            throw diaspora::Exception{"Topic already exists"};
        } else {
            throw diaspora::Exception{fmt::format(
                "Could not create topic \"{}\". "
                "Yokan error: {}",
                name, ex.what())};
        }
    }
    spdlog::trace("[mofka:client] Successfully create topic {}", name);

    // initialize partitions if options required it
    auto num_servers = m_bsgh.size();
    for(int i=0; i < num_partitions; ++i) {
        addCustomPartition(name, i % num_servers, partition_type,
                           partition_config, partition_dependencies,
                           partition_pool);
    }
}

std::shared_ptr<diaspora::TopicHandleInterface> MofkaDriver::openTopic(std::string_view name) const {
    spdlog::trace("[mofka:client] Opening topic {}", name);
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
        spdlog::trace("[mofka:client] Checking key lengths for validator, selector, and serializer");
        m_yk_master_db.lengthMulti(3,
            keysPtrs.data(), ksizes.data(), vsizes.data(), YOKAN_MODE_NO_RDMA);
    } catch(const yokan::Exception& ex) {
        spdlog::trace("[mofka:client] Yokan lengthMulti failed: {}", ex.what());
        throw diaspora::Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Yokan lengthMulti error: {}",
            name, ex.what())};
    } catch(const std::exception& ex) {
        spdlog::trace("[mofka:client] Yokan lengthMulti failed: {}", ex.what());
        throw diaspora::Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Unexpected error from lengthMulti: {}",
            name, ex.what())};
    }
    // if any of the keys is not found, this is a problem
    for(size_t i = 0; i < vsizes.size(); ++i) {
        if(vsizes[i] == YOKAN_KEY_NOT_FOUND) {
            spdlog::trace("[mofka:client] Key \"{}\" not found in master database", keys[i]);
            throw diaspora::Exception{
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
        spdlog::trace("[mofka:client] Getting validator, selector, and serializer from master database");
        m_yk_master_db.getMulti(3,
            keysPtrs.data(), ksizes.data(),
            valuesPtrs.data(), vsizes.data(), YOKAN_MODE_NO_RDMA);
    } catch(const yokan::Exception& ex) {
        spdlog::trace("[mofka:client] Yokan getMulti failed: {}", ex.what());
        throw diaspora::Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Yokan getMulti error: {}",
            name, ex.what())};
    } catch(const std::exception& ex) {
        spdlog::trace("[mofka:client] Yokan getMulti failed: {}", ex.what());
        throw diaspora::Exception{fmt::format(
            "Could not open topic \"{}\". "
            "Unexpected error from getMulti: {}",
            name, ex.what())};
    }

    // deserialize the validator, selector, and serializer from their Metadata
    spdlog::trace("[mofka:client] Instantiating validator");
    auto validator = diaspora::Validator::FromMetadata(diaspora::Metadata{values[0]});
    spdlog::trace("[mofka:client] Instantiating selector");
    auto selector = diaspora::PartitionSelector::FromMetadata(diaspora::Metadata{values[1]});
    spdlog::trace("[mofka:client] Instantiating serializer");
    auto serializer = diaspora::Serializer::FromMetadata(diaspora::Metadata{values[2]});

    // create a Collection object to access the collection of partitions
    spdlog::trace("[mofka:client] opening collection of partitions");
    auto partitionCollection = yokan::Collection{
        fmt::format("MOFKA:GLOBAL:{}:partitions", name).c_str(),
        m_yk_master_db};
    std::vector<diaspora::Metadata> partitionsMetadata;

    // read the partitions from the collection
    try {
        yk_id_t startID = 0;
        std::vector<yk_id_t> ids(32);
        std::vector<char>    docs(32*1024);
        std::vector<size_t>  doc_sizes(32);
        bool done = false;
        while(!done) {
            partitionCollection.listPacked(
                startID, nullptr, 0, 32, ids.data(), docs.size(),
                docs.data(), doc_sizes.data());
            size_t offset = 0;
            for(size_t i = 0; i < 32; ++i) {
                if(doc_sizes[i] == YOKAN_NO_MORE_DOCS) {
                    done = true;
                    break;
                }
                if(doc_sizes[i] > YOKAN_LAST_VALID_SIZE)
                    break;
                auto partitionMetadata = diaspora::Metadata{
                     std::string{static_cast<const char*>(docs.data() + offset), doc_sizes[i]}
                };
                partitionsMetadata.push_back(std::move(partitionMetadata));
                offset += doc_sizes[i];
                startID += 1;
            }
        }
    } catch(const yokan::Exception& ex) {
        throw diaspora::Exception{fmt::format(
            "Could not lookup partitions for topic \"{}\". Yokan iter error: {}",
            name, ex.what())};
    }

    // Deserialize the each partition information from its Metadata.
    // They should have a uuid field, an address field, and
    // a provider_id field.
    std::vector<std::shared_ptr<MofkaPartitionInfo>> partitionsList;
    for(auto& partitionMetadata : partitionsMetadata) {
        const auto& partitionMetadataJson = partitionMetadata.json();
        auto uuid = UUID::from_string(
                partitionMetadataJson["uuid"].get_ref<const std::string&>().c_str());
        const auto& address = partitionMetadataJson["address"].get_ref<const std::string&>();
        uint16_t provider_id = partitionMetadataJson["provider_id"].get<uint16_t>();
        auto partitionInfo = std::make_shared<MofkaPartitionInfo>(
            uuid, thallium::provider_handle{ m_engine.lookup(address), provider_id});
        partitionsList.push_back(std::move(partitionInfo));
    }

    return std::make_shared<MofkaTopicHandle>(
        m_engine, name,
        std::move(validator),
        std::move(selector),
        std::move(serializer),
        std::move(partitionsList),
        const_cast<MofkaDriver*>(this)->shared_from_this());
}

bool MofkaDriver::topicExists(std::string_view name) const {
    try {
        openTopic(name);
    } catch(const diaspora::Exception& ex) {
        if(std::string_view{ex.what()}.find("not found"))
            return false;
        throw;
    }
    return true;
}

void MofkaDriver::addMemoryPartition(std::string_view topic_name,
                                       size_t server_rank,
                                       std::string_view pool_name) {
    addCustomPartition(topic_name, server_rank, "memory", {}, {}, pool_name);
}

void MofkaDriver::addDefaultPartition(std::string_view topic_name,
                                        size_t server_rank,
                                        std::string_view metadata_provider,
                                        std::string_view data_provider,
                                        const diaspora::Metadata& config,
                                        std::string_view pool_name) {
    Dependencies dependencies = {
        {"metadata", {std::string{metadata_provider.data(), metadata_provider.size()}}},
        {"data", {std::string{data_provider.data(), data_provider.size()}}}
    };
    if(metadata_provider.size() == 0 || data_provider.size() == 0) {
        try {
            auto server = m_bsgh[server_rank];
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
                    throw diaspora::Exception(
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
                    throw diaspora::Exception(
                        "No data provider provided or found in server. "
                        "Please provide one or make sure a Warabi provider exists "
                        "with the tag \"mofka:data\" in the server.");
                }
            }

        } catch(const bedrock::Exception& ex) {
            throw diaspora::Exception{
                fmt::format("Error when querying server configuration: {}", ex.what())
            };
        }
    }

    addCustomPartition(topic_name, server_rank, "default", config, dependencies, pool_name);
}

void MofkaDriver::addCustomPartition(
    std::string_view topic_name,
    size_t server_rank,
    std::string_view partition_type,
    const diaspora::Metadata& partition_config,
    const Dependencies& dependencies,
    std::string_view pool_name) {

    auto partition_uuid = UUID::generate();
    auto provider_name  = fmt::format("{}_partition_{}", topic_name, partition_uuid.to_string().substr(0, 8));
    uint16_t provider_id;

    // spin up the provider in the server
    std::string provider_address;
    try {
        auto server = m_bsgh[server_rank];
        provider_address = static_cast<std::string>(server.providerHandle());
        auto provider_desciption = nlohmann::json::object();
        provider_desciption["name"]   = provider_name;
        provider_desciption["type"]   = "mofka";
        provider_desciption["config"] = nlohmann::json::object();
        provider_desciption["config"]["topic"]     = topic_name;
        provider_desciption["config"]["type"]      = partition_type;
        provider_desciption["config"]["uuid"]      = partition_uuid.to_string();
        provider_desciption["config"]["partition"] = partition_config.json();
        provider_desciption["tags"] = nlohmann::json::array();
        provider_desciption["tags"].push_back("morka:partition");
        provider_desciption["dependencies"] = dependencies;
        if(!provider_desciption["dependencies"].contains("pool")) {
            provider_desciption["dependencies"]["pool"] = pool_name.size() ? pool_name : "__primary__";
        }
        provider_desciption["dependencies"]["master_database"] = m_yk_master_info;
        server.addProvider(provider_desciption.dump(), &provider_id);
    } catch(const bedrock::Exception& ex) {
        throw diaspora::Exception{
            fmt::format(
                "Could not create partition for topic \"{}\". "
                "Bedrock error: {}", topic_name, ex.what())
        };
    }

    // add the information about the provider in the master database
    auto partitionCollection = yokan::Collection{
        fmt::format("MOFKA:GLOBAL:{}:partitions", topic_name).c_str(),
        m_yk_master_db};
    try {
        auto partition_info = fmt::format(
            "{{\"address\":\"{}\",\"provider_id\":{},\"uuid\":\"{}\"}}",
            provider_address, provider_id, partition_uuid.to_string()
        );
        partitionCollection.store(partition_info.c_str(), partition_info.size());
    } catch(yokan::Exception& ex) {
        throw diaspora::Exception{
            fmt::format(
                "Could not add partition info to master database for topic \"{}\"."
                "Yokan error: {}", topic_name, ex.what())
        };
    }
}

std::string MofkaDriver::addDefaultMetadataProvider(
          size_t server_rank,
          const diaspora::Metadata& config,
          const Dependencies& dependencies) {
    auto uuid = UUID::generate();
    auto deps_copy = dependencies;
    auto provider_name  = fmt::format("metadata_{}", uuid.to_string().substr(0, 8));
    try {
        auto server = m_bsgh[server_rank];
        auto address = static_cast<std::string>(server.providerHandle());

        auto provider_desciption      = nlohmann::json::object();
        provider_desciption["name"]   = provider_name;
        provider_desciption["type"]   = "yokan";
        provider_desciption["config"] = config.json();
        provider_desciption["tags"]   = nlohmann::json::array();
        provider_desciption["tags"].push_back("mofka:metadata");
        provider_desciption["dependencies"] = dependencies;
        provider_desciption["dependencies"]["master_database"] = m_yk_master_info;
        uint16_t provider_id = 0;
        server.addProvider(provider_desciption.dump(), &provider_id);
        return fmt::format("{}@{}", provider_name, address);
    } catch(const bedrock::Exception& ex) {
        throw diaspora::Exception{
            fmt::format("Could not create metadata provider. Bedrock error: {}", ex.what())
        };
    }
}

std::string MofkaDriver::addDefaultDataProvider(
          size_t server_rank,
          const diaspora::Metadata& config,
          const Dependencies& dependencies) {
    auto uuid = UUID::generate();
    auto provider_name  = fmt::format("data_{}", uuid.to_string().substr(0, 8));
    try {
        auto server = m_bsgh[server_rank];
        auto address = static_cast<std::string>(server.providerHandle());

        auto provider_desciption      = nlohmann::json::object();
        provider_desciption["name"]   = provider_name;
        provider_desciption["type"]   = "warabi";
        provider_desciption["config"] = config.json();
        provider_desciption["tags"]   = nlohmann::json::array();
        provider_desciption["tags"].push_back("mofka:data");
        provider_desciption["dependencies"] = dependencies;
        provider_desciption["dependencies"]["master_database"] = m_yk_master_info;
        uint16_t provider_id = 0;
        server.addProvider(provider_desciption.dump(), &provider_id);
        return fmt::format("{}@{}", provider_name, address);
    } catch(const bedrock::Exception& ex) {
        throw diaspora::Exception{
            fmt::format("Could not create metadata provider. Bedrock error: {}", ex.what())
        };
    }
}

std::shared_ptr<diaspora::ThreadPoolInterface> MofkaDriver::defaultThreadPool() const {
    auto pool = m_engine.get_progress_pool();
    return std::make_shared<MofkaThreadPool>(pool);
}

std::shared_ptr<diaspora::ThreadPoolInterface> MofkaDriver::makeThreadPool(diaspora::ThreadCount count) const {
    return std::make_shared<MofkaThreadPool>(count);
}

void MofkaDriver::startProgressThread() const {
    int ret;
    constexpr auto pool_config = R"({
        "name": "__progress__",
        "type": "fifo_wait",
        "access": "mpmc"
    })";
    constexpr auto xstream_config = R"({
        "name": "__progress__",
        "scheduler": {"type": "basic_wait", "pools": ["__progress__"]},
    })";
    auto mid = m_engine.get_margo_instance();
    margo_pool_info pool_info;
    ret = margo_add_pool_from_json(mid, pool_config, &pool_info);
    if(ret != 0) throw diaspora::Exception{
        fmt::format("Could not start progress thread, margo_add_pool_from_json returned {}", ret)};
    margo_xstream_info xstream_info;
    ret = margo_add_xstream_from_json(mid, xstream_config, &xstream_info);
    if(ret != 0) throw diaspora::Exception{
        fmt::format("Could not start progress thread, margo_add_xstream_from_json returned {}", ret)};
    ret = margo_migrate_progress_loop(mid, pool_info.index);
    if(ret != 0) throw diaspora::Exception{
        fmt::format("Could not start progress thread, margo_migrate_progress_loop returned {}", ret)};
}

}
