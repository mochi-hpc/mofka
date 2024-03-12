#include "StringGenerator.hpp"
#include "MetadataGenerator.hpp"
#include "PropertyListSerializer.hpp"
#include "Producer.hpp"
#include "Consumer.hpp"

#include "../src/JsonUtil.hpp"

#include <bedrock/Server.hpp>

#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <fstream>
#include <iostream>
#include <chrono>
#include <random>
#include <mpi.h>

MOFKA_REGISTER_SERIALIZER(property_list_serializer, PropertyListSerializer);

using json = nlohmann::json;

static const json configSchema = R"(
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "address": {"type": "string"},
        "servers": {
            "type": "object",
            "properties": {
                "ranks": { "type": "array", "minItems": 1, "uniqueItems": true,
                           "items": {"type":"integer", "minimum":0}},
                "config": { "type": "object"}
            },
            "required": ["ranks"]
        },
        "producers": {
            "type": "object",
            "properties": {
                "ranks": { "type": "array", "minItems": 1, "uniqueItems": true,
                           "items": {"type":"integer", "minimum":0}},
                "batch_size": { "oneOf": [
                    {"type": "integer", "minimum": 1},
                    {"enum": ["adaptive"]}
                ]},
                "ordering": {"enum": ["loose", "strict"]},
                "thread_count": {"type": "integer", "minimum": 0},
                "num_events": {"type": "integer", "minimum": 0},
                "topic": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "validator": {"enum": ["default", "schema"]},
                        "partition_selector": {"enum": ["default"]},
                        "serializer": {"enum": ["default", "property_list_serializer"]},
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "num_fields": {"type": "integer", "minimum": 0},
                                "key_sizes": { "oneOf": [
                                    {"type": "integer", "minimum": 4},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 4}}
                                ]},
                                "val_sizes": { "oneOf": [
                                    {"type": "integer", "minimum": 0},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 0}}
                                ]}
                            },
                            "required": ["num_fields", "key_sizes", "val_sizes"]
                        },
                        "data": {
                            "type": "object",
                            "properties": {
                                "num_blocks": { "oneOf": [
                                    {"type": "integer", "minimum": 0},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 0}}
                                ]},
                                "block_size": { "oneOf": [
                                    {"type": "integer", "minimum": 1},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 1}}
                                ]}
                            },
                            "required": [ "num_blocks", "block_size" ]
                        },
                        "partitions": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "oneOf": [
                                {
                                    "type": "object",
                                    "properties": {
                                        "rank": {"type": "integer", "minimum":0},
                                        "type": {"enum": ["memory"]},
                                        "pool": {"type": "string"}
                                    },
                                    "required": ["type", "rank"]
                                },
                                {
                                    "type": "object",
                                    "properties": {
                                        "rank": {"type": "integer", "minimum":0},
                                        "type": {"enum": ["default"]},
                                        "pool": {"type": "string"},
                                        "metadata_provider": {"type": "string"},
                                        "data_provider": {"type": "string"}
                                    },
                                    "required": ["type", "rank"]
                                }
                            ]}
                        }
                    },
                    "required": ["name", "metadata", "partitions"]
                }
            },
            "required": ["topic", "ranks", "num_events"]
        },
        "consumers": {
            "type": "object",
            "properties": {
                "ranks": { "type": "array", "minItems": 1, "uniqueItems": true,
                           "items": {"type":"integer", "minimum":0}},
                "config": {
                    "type": "object",
                    "properties": {
                        "batch_size": { "oneOf": [
                            {"type": "integer", "minimum": 1},
                            {"enum": ["adaptive"]}
                        ]},
                        "thread_count": {"type": "integer", "minimum": 0},
                        "partitions_per_consumer": {"type":"integer", "minimum":1},
                        "data_selector": {
                            "type":"object",
                            "properties": {
                                "selectivity": {"type":"number", "minimum":0, "maximum":1},
                                "fragmentation": { "oneOf": [
                                    {"type": "integer", "minimum": 1},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 1}}
                                ]}
                            }
                        },
                        "data_broker": {
                            "type":"object",
                            "properties": {
                                "reuse": {"type":"boolean"},
                                "fragmentation": { "oneOf": [
                                    {"type": "integer", "minimum": 1},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 1}}
                                ]}
                            }
                        }
                    }
                }
            },
            "required": ["ranks"]
        },
        "options": {
            "type": "object",
            "properties": {
                "concurrent": {"type": "boolean"}
            }
        }
    },
    "required": [
        "address"
    ]
}
)"_json;

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
        exit(-1);
    }

    MPI_Init(&argc, &argv);

    int comm_rank;
    int comm_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    unsigned seed = 12345 + comm_rank;

    json config;

    if(comm_rank == 0) {

        // read the configuration file's content
        std::ifstream configFile(argv[1]);
        if(!configFile.good()) {
            std::cerr << "Could not access file " << argv[1] << std::endl;
            MPI_Abort(MPI_COMM_WORLD, -1);
            return 0;
        }
        std::string configStr;
        configStr.assign(
            (std::istreambuf_iterator<char>(configFile)),
            (std::istreambuf_iterator<char>()));
        config = json::parse(configStr);

        // validate the configuration file against the schema
        mofka::JsonValidator validator{configSchema};
        auto errors = validator.validate(config);
        if(!errors.empty()) {
            std::cerr << "Invalid configuration:\n";
            for(auto& err : errors) std::cerr << err << "\n";
            MPI_Abort(MPI_COMM_WORLD, -1);
            return 0;
        }

        // check that the ranks specified are in the correct range
        auto checkRanks = [comm_size](const json& ranks) {
            for(auto& r : ranks) {
                if(r.get<size_t>() >= (size_t)comm_size) {
                    std::cerr << "Error in configuration: rank " << r.get<size_t>()
                              << " >= size of MPI_COMM_WORLD (" << comm_size << ")\n";
                    MPI_Abort(MPI_COMM_WORLD, -1);
                    return;
                }
            }
        };
        checkRanks(config["servers"]["ranks"]);
        checkRanks(config["producers"]["ranks"]);
        checkRanks(config["consumers"]["ranks"]);

        // share the configuration with other processes
        size_t configSize = configStr.size();
        MPI_Bcast(&configSize, sizeof(configSize), MPI_BYTE, 0, MPI_COMM_WORLD);
        MPI_Bcast(configStr.data(), configStr.size(), MPI_BYTE, 0, MPI_COMM_WORLD);

    } else {

        // get the configuration from rank 0
        std::string configStr;
        size_t configSize = 0;
        MPI_Bcast(&configSize, sizeof(configSize), MPI_BYTE, 0, MPI_COMM_WORLD);
        configStr.resize(configSize);
        MPI_Bcast(configStr.data(), configStr.size(), MPI_BYTE, 0, MPI_COMM_WORLD);
        config = json::parse(configStr);
    }

    // check roles
    bool is_server   = false;
    bool is_producer = false;
    bool is_consumer = false;
    if(config.contains("servers"))
        for(auto& i : config["servers"]["ranks"])
            if(comm_rank == i.get<int>()) is_server = true;
    if(config.contains("producers"))
        for(auto& i : config["producers"]["ranks"])
            if(comm_rank == i.get<int>()) is_producer = true;
    if(config.contains("consumers"))
        for(auto& i : config["consumers"]["ranks"])
            if(comm_rank == i.get<int>()) is_consumer = true;

    auto address = config["address"].get<json::string_t>();

    // instantiating a Bedrock server
    std::shared_ptr<bedrock::Server>   server;
    std::shared_ptr<BenchmarkProducer> producer;
    std::shared_ptr<BenchmarkConsumer> consumer;

    if(is_server) {
        auto server_config = config["server"].contains("config") ?
            config["server"]["config"].dump() : std::string{"{}"};
        server = std::make_shared<bedrock::Server>(address, server_config);
    } else {
        auto server_config = R"({"margo":{"use_progress_thread":true}})";
        server = std::make_shared<bedrock::Server>(address, server_config);
    }

    MPI_Comm server_comm = MPI_COMM_NULL;
    MPI_Comm producer_comm = MPI_COMM_NULL;
    MPI_Comm consumer_comm = MPI_COMM_NULL;
    MPI_Comm_split(MPI_COMM_WORLD, is_server ? 1 : 0, comm_rank, &server_comm);
    MPI_Comm_split(MPI_COMM_WORLD, is_producer ? 1 : 0, comm_rank, &producer_comm);
    MPI_Comm_split(MPI_COMM_WORLD, is_consumer ? 1 : 0, comm_rank, &consumer_comm);

    bool simultaneous = false;
    if(config.contains("options"))
        simultaneous = config["options"].value("simultaneous", false);

    if(is_producer)
        producer = std::make_shared<BenchmarkProducer>(
            seed, config["producers"], producer_comm, is_consumer && simultaneous);
    if(is_consumer)
        consumer = std::make_shared<BenchmarkConsumer>(
            seed + comm_size, config["consumers"], consumer_comm);

    MPI_Barrier(MPI_COMM_WORLD);

    if(is_producer) producer->run();
    if(!simultaneous) MPI_Barrier(MPI_COMM_WORLD);
    if(is_consumer) consumer->run();

    if(is_consumer) consumer->wait();
    if(is_producer) producer->wait();

    MPI_Barrier(MPI_COMM_WORLD);
    server->finalize();

    MPI_Finalize();

    return 0;
}
