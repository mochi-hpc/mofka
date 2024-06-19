#ifndef BENCHMARK_CONSUMER_H
#define BENCHMARK_CONSUMER_H

#include "Communicator.hpp"

#include <mofka/Client.hpp>
#include <mofka/ServiceHandle.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/Consumer.hpp>

#include <nlohmann/json.hpp>
#include <mpi.h>
#include <thallium.hpp>
#include <spdlog/spdlog.h>

class BenchmarkConsumer {

    using json = nlohmann::json;

    thallium::engine     m_engine;
    mofka::Client        m_mofka_client;
    mofka::ServiceHandle m_mofka_service_handle;
    mofka::TopicHandle   m_mofka_topic_handle;
    mofka::Consumer      m_mofka_consumer;
    Communicator         m_comm;
    json                 m_config;
    bool                 m_run_in_thread;
    bool                 m_has_partitions;

    thallium::managed<thallium::pool>    m_run_pool;
    thallium::managed<thallium::xstream> m_run_es;
    thallium::managed<thallium::thread>  m_run_ult;

    public:

    BenchmarkConsumer(
        thallium::engine engine,
        unsigned seed,
        const json& config,
        Communicator comm,
        bool run_in_thread = false)
    : m_engine{std::move(engine)}
    , m_mofka_client{m_engine}
    , m_comm{comm}
    , m_config(config)
    , m_run_in_thread{run_in_thread}
    {
        auto& group_file = config["group_file"].get_ref<const std::string&>();
        spdlog::trace("[consumer] Connecting to mofka file \"{}\"", group_file);
        m_mofka_service_handle = m_mofka_client.connect(group_file);

        auto& topic_name = config["topic_name"].get_ref<const std::string&>();
        spdlog::trace("[consumer] Opening topic \"{}\"", topic_name);
        m_mofka_topic_handle = m_mofka_service_handle.openTopic(topic_name);

        auto consumer_name = config["consumer_name"].get<json::string_t>();
        auto batch_size = mofka::BatchSize::Adaptive();
        if(config.contains("batch_size") && config["batch_size"].is_number_unsigned()) {
            batch_size = mofka::BatchSize{config["batch_size"].get<size_t>()};
        }
        auto thread_pool = mofka::ThreadPool{
            mofka::ThreadCount{config.value("thread_count", (size_t)0)}};

        // TODO add data selector and data broker

        std::vector<mofka::PartitionInfo> my_partitions;
        size_t comm_size = m_comm.size();
        for(size_t i = m_comm.rank(); i < m_mofka_topic_handle.partitions().size(); i += comm_size) {
            my_partitions.push_back(m_mofka_topic_handle.partitions()[i]);
        }
        m_has_partitions = !my_partitions.empty();
        if(m_has_partitions) {
            spdlog::trace("[consumer] Creating consumer \"{}\" associated with {} partitions",
                          consumer_name, my_partitions.size());
            m_mofka_consumer = m_mofka_topic_handle.consumer(
                consumer_name, batch_size, thread_pool, my_partitions);
        } else {
            spdlog::trace("[consumer] Consumer \"{}\" has no partition on this process",
                          consumer_name);
        }
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
        std::unordered_map<mofka::UUID, size_t> events_received;
        auto num_events = m_config["num_events"].get<size_t>();
        auto ack        = m_config.value("ack_every", num_events);
        m_comm.barrier();
        spdlog::info("[consumer] Starting to consume events");
        double t_start = MPI_Wtime();
        if(m_has_partitions) {
            for(size_t i = 0; i < num_events; ++i) {
                spdlog::trace("[consumer] Pulling event {}", i);
                auto event = m_mofka_consumer.pull().wait();
                spdlog::trace("[consumer] Done pulling event {}: received event {} from partition {}",
                             i, event.id(), event.partition().uuid().to_string());
                auto uuid = event.partition().uuid();
                auto it = events_received.find(uuid);
                if(it == events_received.end())
                    it = events_received.insert(std::make_pair(uuid, 0)).first;
                it->second += 1;
                if(it->second % ack == 0 || i == num_events-1) {
                    spdlog::trace("[consumer] Acknowledging event {}", i);
                    event.acknowledge();
                    spdlog::trace("[consumer] Done acknowledging event {}", i);
                }
            }
        }
        double t_end = MPI_Wtime();
        spdlog::info("[consumer] Local consumer finished in {} seconds", (t_end - t_start));
        m_comm.barrier();
        t_end = MPI_Wtime();
        spdlog::info("[consumer] Consumer app finished in {} seconds", (t_end - t_start));
    }
};

#endif
