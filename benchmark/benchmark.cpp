#include "StringGenerator.hpp"
#include "MetadataGenerator.hpp"
#include "PropertyListSerializer.hpp"
#include "Producer.hpp"
#include "Consumer.hpp"
#include "Communicator.hpp"

#include "../src/JsonUtil.hpp"

#include <bedrock/Server.hpp>

#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <fstream>
#include <iostream>
#include <chrono>
#include <random>
#include <spdlog/spdlog.h>
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
                "config": { "oneOf": [ {"type": "object"}, {"type": "array" } ] }
            },
            "required": ["ranks"]
        },
        "producers": {
            "type": "object",
            "properties": {
                "group_file": {"type":"string"},
                "ranks": { "type": "array", "minItems": 1, "uniqueItems": true,
                           "items": {"type":"integer", "minimum":0}},
                "batch_size": { "oneOf": [
                    {"type": "integer", "minimum": 1},
                    {"enum": ["adaptive"]}
                ]},
                "ordering": {"enum": ["loose", "strict"]},
                "thread_count": {"type": "integer", "minimum": 0},
                "num_events": {"type": "integer", "minimum": 0},
                "burst_size": { "oneOf": [
                    {"type": "integer", "minimum": 1},
                    {"type": "array", "minItems":2, "maxItems":2,
                     "items": {"type":"integer", "minimum": 1}}
                ]},
                "wait_between_bursts_ms": { "oneOf": [
                    {"type": "integer", "minimum": 0},
                    {"type": "array", "minItems":2, "maxItems":2,
                     "items": {"type":"integer", "minimum": 0}}
                ]},
                "wait_between_events_ms": { "oneOf": [
                    {"type": "integer", "minimum": 0},
                    {"type": "array", "minItems":2, "maxItems":2,
                     "items": {"type":"integer", "minimum": 0}}
                ]},
                "flush_between_bursts": {"type": "boolean"},
                "flush_every": { "oneOf": [
                    {"type": "integer", "minimum": 1},
                    {"type": "array", "minItems":2, "maxItems":2,
                     "items": {"type":"integer", "minimum": 1}}
                ]},
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
                                "total_size": { "oneOf": [
                                    {"type": "integer", "minimum": 0},
                                    {"type": "array", "minItems":2, "maxItems":2,
                                     "items": {"type":"integer", "minimum": 1}}
                                ]}
                            },
                            "required": [ "num_blocks", "total_size" ]
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
            "required": ["topic", "ranks", "num_events", "group_file"]
        },
        "consumers": {
            "type": "object",
            "properties": {
                "group_file": {"type":"string"},
                "topic_name": {"type":"string"},
                "consumer_name": {"type":"string"},
                "ranks": { "type": "array", "minItems": 1, "uniqueItems": true,
                           "items": {"type":"integer", "minimum":0}},
                "num_events": {"type": "integer", "minimum": 0},
                "batch_size": { "oneOf": [
                    {"type": "integer", "minimum": 1},
                    {"enum": ["adaptive"]}
                ]},
                "check_data": {"type": "boolean"},
                "thread_count": {"type": "integer", "minimum": 0},
                "data_selector": {
                    "type":"object",
                    "properties": {
                        "selectivity": {"type":"number", "minimum":0, "maximum":1},
                        "proportion": { "oneOf": [
                            {"type": "number", "minimum": 0, "maximum": 1},
                            {"type": "array", "minItems":2, "maxItems":2,
                             "items": {"type":"number", "minimum": 0, "maximum": 1}}
                        ]}
                    }
                },
                "data_broker": {
                    "type":"object",
                    "properties": {
                        "reuse": {"type":"boolean"},
                        "num_blocks": { "oneOf": [
                            {"type": "integer", "minimum": 1},
                            {"type": "array", "minItems":2, "maxItems":2,
                             "items": {"type":"integer", "minimum": 1}}
                        ]}
                    }
                }
            },
            "required": ["ranks", "group_file", "topic_name", "consumer_name"]
        },
        "options": {
            "type": "object",
            "properties": {
                "simultaneous": {"type": "boolean"}
            }
        }
    },
    "required": [
        "address"
    ]
}
)"_json;

static json expandSimplifiedJSON(const json& input);

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <results.json>" << std::endl;
        exit(-1);
    }

    //spdlog::set_level(spdlog::level::from_str("trace"));

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);
    if(provided != MPI_THREAD_SERIALIZED) {
        std::cerr << "WARNING: MPI_THREAD_SERIALIZED not supported" << std::endl;
    }

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
        config = expandSimplifiedJSON(json::parse(configStr, nullptr, true, true));
        configStr = config.dump();

        // validate the configuration file against the schema
        mofka::JsonSchemaValidator validator{configSchema};
        auto errors = validator.validate(config);
        if(!errors.empty()) {
            std::cerr << "Invalid configuration: ";
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

    auto server_config = std::string{"{}"};
    if(is_server) {
        if(config["servers"].contains("config")) {
            auto& cfg = config["servers"]["config"];
            if(cfg.is_object()) server_config = cfg.dump();
            else server_config = cfg[comm_rank].dump();
        }
    } else {
        server_config = R"({
            "margo": {
                "use_progress_thread": true
            }
        })";
    }
    server = std::make_shared<bedrock::Server>(address, server_config);

    // register a warmup RPC
    auto engine = server->getMargoManager().getThalliumEngine();
    auto warmup = engine.define("warmup", std::function{[](const thallium::request& req){
        req.respond();
    }});

    server->getMargoManager().addPool(
        R"({"name":"__mpi_pool__", "kind":"fifo_wait", "access":"mpmc"})");
    server->getMargoManager().addXstream(
        R"({"name":"__mpi_xstream__", "scheduler":{"pools":["__mpi_pool__"],"type":"basic_wait"}}})");

    auto mpi_pool =
        server->getMargoManager().getPool("__mpi_pool__")->getHandle<thallium::pool>();

    auto world = Communicator{mpi_pool, MPI_COMM_WORLD};
    world.barrier();

    // exchange addresses
    std::vector<char> addresses(256*world.size());
    auto myaddress = static_cast<std::string>(engine.self());
    myaddress.resize(256, '\0');
    world.allgather(myaddress.c_str(), 256, addresses.data());

    // send a warmup RPC
    for(int i = 0; i < world.size(); ++i) {
        int j = (i + world.rank()) % world.size();
        const char* addr = addresses.data() + 256*j;
        warmup.on(engine.lookup(addr))();
    }
    world.barrier();

    auto server_comm   = world.split(is_server ? 1 : 0);
    auto producer_comm = world.split(is_producer ? 1 : 0);;
    auto consumer_comm = world.split(is_consumer ? 1 : 0);

    bool simultaneous = false;
    if(config.contains("options"))
        simultaneous = config["options"].value("simultaneous", false);

    if(is_producer)
        producer = std::make_shared<BenchmarkProducer>(
            server->getMargoManager().getThalliumEngine(),
            seed, config["producers"], producer_comm, is_consumer && simultaneous);
    world.barrier();
    if(is_consumer)
        consumer = std::make_shared<BenchmarkConsumer>(
            server->getMargoManager().getThalliumEngine(),
            seed + comm_size, config["consumers"], consumer_comm);

    world.barrier();

    if(is_producer) producer->run();
    if(!simultaneous) world.barrier();
    if(is_consumer) consumer->run();

    if(is_consumer) consumer->wait();
    if(is_producer) producer->wait();

    world.barrier();

    // create local result JSON structure
    json local_result = json::object();
    if(is_producer) local_result["producer"] = producer->getStatistics();
    if(is_consumer) local_result["consumer"] = consumer->getStatistics();

    // aggregate results across ranks
    auto local_result_str = local_result.dump();
    unsigned long local_result_size = local_result_str.size();
    unsigned long max_local_result_size;
    world.allreduce(&local_result_size, &max_local_result_size, 1, MPI_UNSIGNED_LONG, MPI_MAX);
    local_result_str.resize(max_local_result_size+1, '\0');
    std::vector<char> all_results((max_local_result_size+1)*world.size());
    world.allgather(local_result_str.data(), local_result_str.size(), all_results.data());
    if(world.rank() == 0) {
        json result = json::object();
        for(int i=0; i < world.size(); ++i) {
            const char* addr = addresses.data() + 256*i;
            result[addr] = json::parse(all_results.data() + i*(max_local_result_size+1));
        }
        std::ofstream{argv[2]} << result;
    }

    producer.reset();
    consumer.reset();

    world.barrier();

    server->finalize();
    server.reset();

    server_comm.free();
    producer_comm.free();
    consumer_comm.free();

    MPI_Finalize();

    return 0;
}

static void expand_json(const json& input, json& output) {
    for (auto it = input.begin(); it != input.end(); ++it) {
        std::istringstream key_stream(it.key());
        std::string segment;
        json* current = &output;

        while (std::getline(key_stream, segment, '.')) {
            if (!key_stream.eof()) {
                if (current->find(segment) == current->end() || !(*current)[segment].is_object()) {
                    (*current)[segment] = json::object();
                }
                current = &(*current)[segment];
            } else {
                if (it.value().is_object()) {
                    expand_json(it.value(), (*current)[segment]);
                } else {
                    (*current)[segment] = it.value();
                }
            }
        }
    }
}

static json expandSimplifiedJSON(const json& input) {
    json output = json::object();
    expand_json(input, output);
    return output;
}
