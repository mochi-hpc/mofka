/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_WARABI_DATA_STORE_HPP
#define MOFKA_WARABI_DATA_STORE_HPP

#include <valijson/adapters/rapidjson_adapter.hpp>
#include <mofka/DataStore.hpp>
#include <warabi/Client.hpp>
#include <warabi/TargetHandle.hpp>
#include <valijson/schema.hpp>
#include <valijson/schema_parser.hpp>
#include <valijson/validator.hpp>
#include <spdlog/spdlog.h>
#include <cstddef>
#include <string_view>
#include <unordered_map>

namespace mofka {

/* Schema to validate the configuration of a WarabiDataStore */
static constexpr const char* configSchema = R"(
{
  "$schema": "https://json-schema.org/draft/2019-09/schema",
  "type": "object",
  "properties": {
     "address": {
       "type": "string"
     },
     "provider_id": {
        "type": "integer",
        "minimum": 0,
        "exclusiveMaximum": 65535
     }
   },
   "required": [
     "address",
     "provider_id"
   ]
}
)";

class WarabiDataStore : public DataStore {

    struct WarabiDataDescriptor {
        size_t           offset;
        warabi::RegionID region_id;
    };

    thallium::engine     m_engine;
    Metadata             m_config;
    warabi::TargetHandle m_target;

    public:

    WarabiDataStore(
            thallium::engine engine,
            Metadata config,
            warabi::TargetHandle target)
    : m_engine(std::move(engine))
    , m_config(std::move(config))
    , m_target(std::move(target)) {}

    Result<std::vector<DataDescriptor>> store(
            size_t count,
            const BulkRef& remoteBulk) override {

        /* prepare the result array and its content */
        Result<std::vector<DataDescriptor>> result;
        result.value().resize(count);
        for(auto& descriptor : result.value()) {
            auto& location = descriptor.location();
            location.resize(sizeof(WarabiDataDescriptor));
        }

        auto getWarabiDataDescriptor = [&result](size_t i) {
            return reinterpret_cast<WarabiDataDescriptor*>(
                result.value()[i].location().data()
            );
        };

        const auto source = m_engine.lookup(remoteBulk.address);

        /* transfer size of each data piece */
        const auto dataOffset = count*sizeof(size_t);

        std::vector<size_t> sizes(count);
        auto sizesBulk = m_engine.expose(
            {{sizes.data(), dataOffset}},
            thallium::bulk_mode::write_only);

        sizesBulk << remoteBulk.handle.on(source)(remoteBulk.offset, dataOffset);

        /* forward data as region into Warabi */
        warabi::RegionID region_id;
        m_target.createAndWrite(
            &region_id, remoteBulk.handle, remoteBulk.address,
            remoteBulk.offset + dataOffset, remoteBulk.size - dataOffset, true);

        /* update the result vector */
        size_t currentOffset = 0;
        for(size_t j = 0; j < count; ++j) {
            auto descriptor = getWarabiDataDescriptor(j);
            descriptor->region_id = region_id;
            descriptor->offset = currentOffset;
            currentOffset += sizes[j];
        }

        return result;
    }

    std::vector<Result<void>> load(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& remoteBulk) override {

        std::vector<Result<void>> result;
        result.resize(descriptors.size());

        auto getWarabiDataDescriptor = [&descriptors](size_t i) {
            return reinterpret_cast<const WarabiDataDescriptor*>(
                        descriptors[i].location().data()
            );
        };

        // issue all the reads in parallel
        // FIXME: it would be better to be able to group by region but this would mean
        // having an API for fragmented region to fragmented bulk in Warabi
        // FIXME: the code bellow doesn't work if the DataDescriptor is not just a location
        std::vector<warabi::AsyncRequest> requests(descriptors.size());
        size_t currentOffset = remoteBulk.offset;
        for(size_t i = 0; i < descriptors.size(); ++i) {
            const auto descriptor     = getWarabiDataDescriptor(i);
            const auto region         = descriptor->region_id;
            const auto offsetInRegion = descriptor->offset;
            const auto size           = descriptors[i].size();
            m_target.read(region, {{offsetInRegion, size}},
                          remoteBulk.handle,
                          remoteBulk.address,
                          currentOffset,
                          &requests[i]);
            currentOffset += size;
        }

        // wait for all the requests
        for(size_t i = 0; i < requests.size(); ++i) {
            try {
                requests[i].wait();
            } catch(const warabi::Exception& ex) {
                result[i].success() = false;
                result[i].error() = ex.what();
            }
        }

        return result;

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
            spdlog::error("[mofka] Error(s) while validating JSON config for WarabiDataStore:");
            for(auto& error : validationResults) {
                spdlog::error("[mofka] \t{}", error.description);
            }
            return nullptr;
        }

        /* Lookup Warabi target */
        auto client          = warabi::Client{engine};
        auto address         = config.json()["address"].GetString();
        uint16_t provider_id = config.json()["provider_id"].GetUint();
        auto target          = client.makeTargetHandle(address, provider_id);

        return std::make_unique<WarabiDataStore>(engine, config, std::move(target));
    }

};

} // namespace mofka

#endif
