/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BAKE_DATA_STORE_HPP
#define MOFKA_BAKE_DATA_STORE_HPP

#include <valijson/adapters/rapidjson_adapter.hpp>
#include <mofka/DataStore.hpp>
#include <bake-client.hpp>
#include <valijson/schema.hpp>
#include <valijson/schema_parser.hpp>
#include <valijson/validator.hpp>
#include <spdlog/spdlog.h>

namespace mofka {

/* Schema to validate the configuration of a BakeDataStore */
static constexpr const char* configSchema = R"(
{
  "$schema": "https://json-schema.org/draft/2019-09/schema",
  "type": "object",
  "properties": {
    "targets": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "address": {
            "type": "string"
          },
          "provider_id": {
            "type": "integer",
            "minimum": 0,
            "exclusiveMaximum": 65535
          },
          "target_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
          }
        },
        "required": [
          "address",
          "provider_id",
          "target_id"
        ]
      },
      "minItems": 1
    }
  },
  "required": [
    "targets"
  ]
}
)";

class BakeDataStore : public DataStore {

    struct BakeTarget {
        bake::provider_handle m_ph;
        bake::target          m_target;
    };

    struct BakeReplicaDescriptor {
        bake::target m_target_id;
        bake::region m_region_id;
    };

    struct BakeDataDescriptor {
        size_t                offset;
        BakeReplicaDescriptor replicas[1];
    };

    thallium::engine        m_engine;
    Metadata                m_config;
    bake::client            m_bake_client;
    std::vector<BakeTarget> m_bake_targets;

    public:

    BakeDataStore(
            thallium::engine engine,
            Metadata config,
            bake::client bake_client,
            std::vector<BakeTarget> bake_targets)
    : m_engine(std::move(engine))
    , m_config(std::move(config))
    , m_bake_client(std::move(bake_client))
    , m_bake_targets(std::move(bake_targets)) {}

    Result<std::vector<DataDescriptor>> store(
            size_t count,
            const BulkRef& remoteBulk) override {
        Result<std::vector<DataDescriptor>> result;

        size_t numReplicas = m_bake_targets.size();
        std::vector<std::vector<char>> descriptorStrings(count);
        for(auto& descriptorString : descriptorStrings)
            descriptorString.resize(sizeof(size_t) + numReplicas*sizeof(BakeReplicaDescriptor));

        auto source = m_engine.lookup(remoteBulk.address);

        size_t s = count*sizeof(size_t);

        std::vector<size_t> sizes(count);
        auto sizesBulk = m_engine.expose(
            {{sizes.data(), s}},
            thallium::bulk_mode::write_only);

        sizesBulk << remoteBulk.handle.on(source)(remoteBulk.offset, s);

        auto createWritePersist = [this, &sizes, s, count, &remoteBulk, &descriptorStrings](size_t i) {
            auto& target = m_bake_targets[i];
            auto region_id = m_bake_client.create_write_persist(
                    target.m_ph,
                    target.m_target,
                    remoteBulk.handle.get_bulk(),
                    remoteBulk.offset + s,
                    remoteBulk.address,
                    remoteBulk.size - s);
            size_t dataOffset = 0;
            for(size_t j=0; j < count; ++j) {
                auto& descriptorString = descriptorStrings[j];
                auto descriptor = reinterpret_cast<BakeDataDescriptor*>(descriptorString.data());
                if(i == 0) descriptor->offset = dataOffset;
                descriptor->replicas[i].m_target_id = target.m_target;
                descriptor->replicas[i].m_region_id = region_id;
                dataOffset += sizes[j];
            }
        };

        std::vector<thallium::managed<thallium::thread>> ults;
        ults.reserve(m_bake_targets.size());
        for(size_t i = 0; i < m_bake_targets.size(); ++i) {
            ults.push_back(
                thallium::thread::self().get_last_pool().make_thread(
                    [i, &createWritePersist](){ createWritePersist(i); })
            );
        }
        for(auto& ult : ults) ult->join();

        for(size_t j = 0; j < count; ++j) {
            auto& bakeDescriptor = descriptorStrings[j];
            auto descriptor = DataDescriptor::From(
                std::string_view{bakeDescriptor.data(), bakeDescriptor.size()}, sizes[j]);
            result.value().push_back(descriptor);
        }

        return result;
    }

    std::vector<Result<void>> load(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& remoteBulk) override {
        std::vector<Result<void>> result;
        result.resize(descriptors.size());

        // get BakeDataDescriptors from the DataDescriptors
        std::vector<const BakeDataDescriptor*> bakeDescriptors;
        std::vector<size_t> numReplicas;
        bakeDescriptors.reserve(descriptors.size());
        numReplicas.resize(descriptors.size());
        for(size_t i = 0; i < descriptors.size(); ++i) {
            const auto& descriptor = descriptors[i];
            bakeDescriptors.push_back(
                reinterpret_cast<const BakeDataDescriptor*>(
                    descriptor.location().data()));
            numReplicas.push_back(
                (descriptor.location().size() - sizeof(size_t))/(sizeof(BakeReplicaDescriptor)));
        }

        auto readFromBake = [&](size_t i) {
            // find a replica to get the data from
            BakeTarget* target = nullptr;
            const bake::region* region_id = nullptr;
            auto& bakeDescriptor = bakeDescriptors[i];
            for(unsigned j=0; j < numReplicas[i]; ++j) {
                auto& r = bakeDescriptor->replicas[j];
                for(auto& t : m_bake_targets) {
                    if(std::memcmp(&t.m_target, &r.m_target_id, sizeof(t.m_target)) == 0) {
                        target = &t;
                        region_id = &r.m_region_id;
                        break;
                    }
                }
                if(target != nullptr) break;
            }
            // check if a replica was found
            if(target == nullptr) {
                result[i].success() = false;
                result[i].error() = "Could not find replica for requested bake DataDescriptor";
                return;
            }
            // read the data
            m_bake_client.read(
                target->m_ph,
                target->m_target,
                *region_id,
                bakeDescriptor->offset,
                remoteBulk.handle.get_bulk(),
                remoteBulk.offset,
                remoteBulk.address,
                descriptors[i].size());
        };

        for(size_t i = 0; i < descriptors.size(); ++i) {
            readFromBake(i);
        }
        return result;
    }

    Result<bool> destroy() override {
        // TODO
        return Result<bool>{true};
    }

    static std::unique_ptr<DataStore> create(
        const thallium::engine& engine,
        const mofka::Metadata& config) {

        /* Validate configuration against schema */

        static rapidjson::Document schemaDocument;
        if(schemaDocument.Empty()) {
            schemaDocument.Parse(configSchema);
        }

        valijson::Schema schemaValidator;
        valijson::SchemaParser schemaParser;
        valijson::adapters::RapidJsonAdapter schemaAdapter(schemaDocument);
        schemaParser.populateSchema(schemaAdapter, schemaValidator);

        valijson::Validator validator;
        valijson::adapters::RapidJsonAdapter jsonAdapter(config.json());

        valijson::ValidationResults validationResults;
        validator.validate(schemaValidator, jsonAdapter, &validationResults);

        if(validationResults.numErrors() != 0) {
            spdlog::error("[mofka] Error(s) while validating JSON config for BakeDataStore:");
            for(auto& error : validationResults) {
                spdlog::error("[mofka] \t{}", error.description);
            }
            return nullptr;
        }

        /* Lookup Bake targets */

        auto bake_client = bake::client{engine.get_margo_instance()};
        std::vector<BakeTarget> bake_targets;
        const auto& json_targets = config.json()["targets"];
        for(const auto& t : json_targets.GetArray()) {
            std::string address     = t["address"].GetString();
            uint16_t    provider_id = (uint16_t)t["provider_id"].GetUint();
            std::string target_uuid = t["target_id"].GetString();
            auto endpoint = engine.lookup(address);
            bake::provider_handle bake_ph{bake_client, endpoint.get_addr(false), provider_id};
            bake_targets.push_back({bake_ph, bake::target(target_uuid)});
        }

        return std::make_unique<BakeDataStore>(
                engine, config, std::move(bake_client), std::move(bake_targets));
    }

};

} // namespace mofka

#endif
