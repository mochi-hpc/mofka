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

    struct BakeDescriptor {
        bake::region m_region;
        size_t       m_offset;

        std::string_view toString() const {
            return std::string_view{
                reinterpret_cast<const char*>(this),
                sizeof(*this)};
        }
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

    RequestResult<std::vector<DataDescriptor>> store(
            size_t count,
            const BulkRef& remoteBulk) override {
        RequestResult<std::vector<DataDescriptor>> result;

        auto source = m_engine.lookup(remoteBulk.address);

        size_t s = count*sizeof(size_t);

        std::vector<size_t> sizes(count);
        auto sizesBulk = m_engine.expose(
            {{sizes.data(), s}},
            thallium::bulk_mode::write_only);

        sizesBulk << remoteBulk.handle.on(source)(remoteBulk.offset, s);

        auto& target = m_bake_targets[0];
        // TODO handle multiple targets

        auto region_id = m_bake_client.create_write_persist(
                target.m_ph,
                target.m_target,
                remoteBulk.handle.get_bulk(),
                remoteBulk.offset + s,
                remoteBulk.address,
                remoteBulk.size - s);

        auto bake_descriptor = BakeDescriptor{region_id, 0};
        for(size_t i = 0; i < count; ++i) {
            auto descriptor = DataDescriptor::From(
                bake_descriptor.toString(), sizes[i]);
            result.value().push_back(descriptor);
            bake_descriptor.m_offset += sizes[i];
        }

        return result;
    }

    RequestResult<void> load(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& dest) override {
        RequestResult<void> result;
#if 0
        // lock the BakeDataStore
        auto guard = std::unique_lock<thallium::mutex>{m_mutex};

        // convert descriptors into pointer/size pairs
        std::vector<std::pair<void*, size_t>> segments;
        segments.reserve(descriptors.size());
        for(auto& d : descriptors) {
            if(d.size() == 0) continue;
            size_t offset;
            std::memcpy(&offset, d.location().data(), sizeof(offset));
            segments.push_back({
                static_cast<void*>(m_data.data() + offset),
                d.size()
            });
        }

        // expose the m_data vector for bulk transfer
        auto localDataBulk = m_engine.expose(
            segments,
            thallium::bulk_mode::read_only);

        // do the bulk transfer
        auto sender = m_engine.lookup(dest.address);
        dest.handle.on(sender)(dest.offset, dest.size) << localDataBulk;
#endif
        return result;
    }

    RequestResult<bool> destroy() override {
#if 0
        // lock the BakeDataStore
        auto guard = std::unique_lock<thallium::mutex>{m_mutex};
        // remove all the data
        m_sizes.clear();
        m_data.clear();
#endif
        return RequestResult<bool>{true};
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
