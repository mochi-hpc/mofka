#ifndef BENCHMARK_PRODUCER_H
#define BENCHMARK_PRODUCER_H

#include "MetadataGenerator.hpp"
#include "Communicator.hpp"

#include <mofka/Client.hpp>
#include <mofka/ServiceHandle.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/Producer.hpp>

#include <nlohmann/json.hpp>
#include <mpi.h>
#include <thallium.hpp>

class BenchmarkProducer {

    using json = nlohmann::json;

    thallium::engine     m_engine;
    mofka::Client        m_mofka_client;
    mofka::ServiceHandle m_mofka_service_handle;
    mofka::TopicHandle   m_mofka_topic_handle;
    mofka::Producer      m_mofka_producer;
    StringGenerator      m_string_generator;
    MetadataGenerator    m_metadata_generator;
    bool                 m_run_in_thread;
    json                 m_config;

    thallium::managed<thallium::pool>    m_run_pool;
    thallium::managed<thallium::xstream> m_run_es;
    thallium::managed<thallium::thread>  m_run_ult;

    public:

    BenchmarkProducer(
        thallium::engine engine,
        unsigned seed,
        const json& config,
        Communicator comm,
        bool run_in_thread = false)
    : m_engine{std::move(engine)}
    , m_mofka_client{m_engine}
    , m_string_generator(seed)
    , m_metadata_generator(
        m_string_generator,
        config["topic"]["metadata"]["num_fields"],
        getMin(config["topic"]["metadata"]["key_sizes"]),
        getMax(config["topic"]["metadata"]["key_sizes"]),
        getMin(config["topic"]["metadata"]["val_sizes"]),
        getMax(config["topic"]["metadata"]["val_sizes"]))
    , m_run_in_thread(run_in_thread)
    , m_config(config)
    {
        m_mofka_service_handle = m_mofka_client.connect(
            mofka::SSGFileName{config["group_file"].get<std::string>()}
        );

        int rank = comm.rank();
        if(rank == 0) {
            createTopic(config["topic"]);
        }
        comm.barrier();

        m_mofka_topic_handle = m_mofka_service_handle.openTopic(
            config["topic"]["name"].get_ref<const json::string_t&>()
        );

        auto batch_size = mofka::BatchSize::Adaptive();
        if(config.contains("batch_size") && config["batch_size"].is_number_unsigned()) {
            batch_size = mofka::BatchSize{config["batch_size"].get<size_t>()};
        }
        auto ordering = mofka::Ordering::Loose;
        if(config.contains("ordering") && config["ordering"] == "strict")
            ordering = mofka::Ordering::Strict;
        auto thread_pool = mofka::ThreadPool{
            mofka::ThreadCount{config.value("thread_count", (size_t)0)}};

        m_mofka_producer = m_mofka_topic_handle.producer(
            batch_size, ordering, thread_pool);
    }

    void run() {
        if(m_run_in_thread) {
            m_run_pool = thallium::pool::create(
                thallium::pool::access::mpmc,
                thallium::pool::kind::fifo_wait);
            std::vector<thallium::pool> pools{*m_run_pool};
            thallium::xstream::create(
                thallium::scheduler::predef::basic_wait,
                pools.begin(), pools.end());
        } else {
            m_run_ult = thallium::xstream::self().make_thread([this](){runThread();});
        }
    }

    void wait() {
        m_run_ult->join();
        m_run_ult = decltype(m_run_ult){};
        if(!m_run_es->is_null()) m_run_es->join();
        m_run_es = decltype(m_run_es){};
        if(!m_run_pool->is_null()) m_run_pool = decltype(m_run_pool){};
    }

    private:

    void runThread() {
        auto num_events = m_config["num_events"].get<size_t>();
        // TODO barrier + timing
        for(size_t i = 0; i < num_events; ++i) {
            auto metadata = m_metadata_generator.generate();
            // TODO handle data
            m_mofka_producer.push(metadata);
            // TODO add push interval and flush frequency
        }
        std::cerr << "Flushing" << std::endl;
        m_mofka_producer.flush();
        // TODO barrier + timing
        std::cerr << "Done" << std::endl;
    }

    void createTopic(const json& config) {
        const auto& name                   = config["name"].get_ref<const json::string_t&>();
        const auto validator_type          = config.value("validator", "default");
        const auto partition_selector_type = config.value("partition_selector", "default");
        const auto serializer_type         = config.value("serializer", "default");

        auto validator_config = json::object();
        if(validator_type == "schema")
            validator_config["schema"] = m_metadata_generator.schema();
        auto partition_selector_config = json::object();
        auto serializer_config = json::object();
        if(serializer_type == "property_list_serializer")
            serializer_config["properties"] = m_metadata_generator.properties();

        m_mofka_service_handle.createTopic(
            name,
            mofka::Validator::FromMetadata(validator_type.c_str(), validator_config),
            mofka::PartitionSelector::FromMetadata(partition_selector_type.c_str(), partition_selector_config),
            mofka::Serializer::FromMetadata(serializer_type.c_str(), serializer_config));

        for(auto& partition : config["partitions"]) {
            auto rank  = partition["rank"].get<unsigned>();
            auto& type = partition["type"];
            auto pool  = partition.value("pool", "__primary__");
            if(type == "memory") {
                m_mofka_service_handle.addMemoryPartition(name, rank, pool);
            } else { // type == "default"
                auto metadata_provider = partition.value("metadata_provider", "");
                auto data_provider = partition.value("data_provider", "");
                auto manager_config = mofka::Metadata{};
                m_mofka_service_handle.addDefaultPartition(
                    name, rank, metadata_provider, data_provider, manager_config, pool);
            }
        }
    }

    static size_t getMin(const json& v) {
        if(v.is_number_unsigned()) {
            return v.get<size_t>();
        } else {
            return std::min<size_t>(
                v[0].get<size_t>(),
                v[1].get<size_t>());
        }
    }

    static size_t getMax(const json& v) {
        if(v.is_number_unsigned()) {
            return v.get<size_t>();
        } else {
            return std::max<size_t>(
                v[0].get<size_t>(),
                v[1].get<size_t>());
        }
    }
};

#endif
