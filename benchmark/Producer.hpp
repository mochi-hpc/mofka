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
#include <unistd.h>
#include <spdlog/spdlog.h>


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
    Communicator         m_comm;

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
    , m_comm(comm)
    {
        auto& group_file = config["group_file"].get_ref<const std::string&>();
        spdlog::trace("[producer] Connecting to mofka file \"{}\"", group_file);
        m_mofka_service_handle = m_mofka_client.connect(group_file);

        auto& topic_name = config["topic"]["name"].get_ref<const std::string&>();
        int rank = comm.rank();
        if(rank == 0) {
            spdlog::trace("[producer] Creating topic \"{}\"", topic_name);
            createTopic(config["topic"]);
        }
        comm.barrier();

        spdlog::trace("[producer] Opening topic \"{}\"", topic_name);
        m_mofka_topic_handle = m_mofka_service_handle.openTopic(topic_name);

        auto batch_size = mofka::BatchSize::Adaptive();
        if(config.contains("batch_size") && config["batch_size"].is_number_unsigned()) {
            batch_size = mofka::BatchSize{config["batch_size"].get<size_t>()};
        }
        auto ordering = mofka::Ordering::Loose;
        if(config.contains("ordering") && config["ordering"] == "strict")
            ordering = mofka::Ordering::Strict;
        auto thread_pool = mofka::ThreadPool{
            mofka::ThreadCount{config.value("thread_count", (size_t)0)}};

        spdlog::trace("[producer] Creating mofka::Producer for topic \"{}\"", topic_name);
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
        auto& rng = m_string_generator.rng();
        auto num_events = m_config["num_events"].get<size_t>();

        size_t min_burst_size = m_config.contains("burst_size") ?
            getMin(m_config["burst_size"]) : 1;
        size_t max_burst_size = m_config.contains("burst_size") ?
            getMax(m_config["burst_size"]) : 1;
        std::uniform_int_distribution<size_t> burst_size_dist(
                min_burst_size, max_burst_size);

        size_t min_wait_between_bursts_ms = m_config.contains("wait_between_bursts_ms") ?
            getMin(m_config["wait_between_bursts_ms"]) : 0;
        size_t max_wait_between_bursts_ms = m_config.contains("wait_between_bursts_ms") ?
            getMax(m_config["wait_between_bursts_ms"]) : 0;
        std::uniform_int_distribution<size_t> wait_between_bursts_ms_dist(
                min_wait_between_bursts_ms, max_wait_between_bursts_ms);

        size_t min_wait_between_events_ms = m_config.contains("wait_between_events_ms") ?
            getMin(m_config["wait_between_events_ms"]) : 0;
        size_t max_wait_between_events_ms = m_config.contains("wait_between_events_ms") ?
            getMax(m_config["wait_between_events_ms"]) : 0;
        std::uniform_int_distribution<size_t> wait_between_events_ms_dist(
                min_wait_between_events_ms, max_wait_between_events_ms);

        size_t min_flush_every = m_config.contains("flush_every") ?
            getMin(m_config["flush_every"]) : 0;
        size_t max_flush_every = m_config.contains("flush_every") ?
            getMax(m_config["flush_every"]) : 0;
        std::uniform_int_distribution<size_t> flush_every_dist(
                min_flush_every, max_flush_every);

        bool flush_between_bursts = m_config.value("flush_between_bursts", false);

        m_comm.barrier();
        spdlog::info("[producer] Starting to produce events");
        double t_start = MPI_Wtime();
        size_t next_burst = burst_size_dist(rng);
        size_t next_flush = flush_every_dist(rng);

        for(size_t i = 0; i < num_events; ++i) {
            auto metadata = m_metadata_generator.generate();
            // TODO handle data
            m_mofka_producer.push(metadata);
            spdlog::trace("[producer] Pushing event {}", i);
            // TODO add push interval and flush frequency
            next_burst -= 1;
            if(next_burst == 0) {
                if(flush_between_bursts) {
                    spdlog::trace("[producer] Flushing after burst of events");
                    m_mofka_producer.flush();
                    spdlog::trace("[producer] Done flushing after burst of events");
                }
                size_t wait_between_bursts_ms = wait_between_bursts_ms_dist(rng);
                if(wait_between_bursts_ms) {
                    spdlog::trace("[producer] Waiting {} msec after burst", wait_between_bursts_ms);
                    usleep(1000*wait_between_bursts_ms);
                    spdlog::trace("[producer] Done waiting after burst", wait_between_bursts_ms);
                }
                next_burst = burst_size_dist(rng);
            } else {
                size_t wait_between_events_ms = wait_between_events_ms_dist(rng);
                if(wait_between_events_ms) {
                    spdlog::trace("[producer] Waiting {} msec after event", wait_between_events_ms);
                    usleep(1000*wait_between_events_ms);
                    spdlog::trace("[producer] Done waiting after event");
                }
            }
            if(max_flush_every != 0) {
                next_flush -= 1;
                if(next_flush == 0) {
                    spdlog::trace("[poducer] Random flush");
                    next_flush = flush_every_dist(rng);
                    m_mofka_producer.flush();
                    spdlog::trace("[poducer] Done with random flush");
                }
            }
        }
        spdlog::trace("[poducer] Last flush");
        m_mofka_producer.flush();
        spdlog::trace("[poducer] Done with last flush");
        double t_end = MPI_Wtime();
        spdlog::info("[producer] Local producer finished in {} seconds", (t_end - t_start));
        m_comm.barrier();
        t_end = MPI_Wtime();
        spdlog::info("[producer] Producer app finished in {} seconds", (t_end - t_start));
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
