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
        m_mofka_service_handle = m_mofka_client.connect(
            config["group_file"].get<std::string>()
        );
        m_mofka_topic_handle = m_mofka_service_handle.openTopic(
            config["topic_name"].get_ref<const json::string_t&>()
        );

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
            m_mofka_consumer = m_mofka_topic_handle.consumer(
                consumer_name, batch_size, thread_pool, my_partitions);
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
        double t_start = MPI_Wtime();
        if(m_has_partitions) {
            for(size_t i = 0; i < num_events; ++i) {
                auto event = m_mofka_consumer.pull().wait();
                std::cout << "Event " << event.id() << " received from partition "
                    << event.partition() << std::endl;
                auto uuid = event.partition().uuid();
                auto it = events_received.find(uuid);
                if(it == events_received.end())
                    it = events_received.insert(std::make_pair(uuid, 0)).first;
                it->second += 1;
                if(it->second % ack == 0 || i == num_events-1) {
                    std::cout << "Acknowledging event " << event.id()
                              << " from partition " << uuid << std::endl;
                    event.acknowledge();
                }
            }
        }
        m_comm.barrier();
        double t_end = MPI_Wtime();
        std::cout << "Consumer finished in " << (t_end - t_start) << " seconds" << std::endl;
    }
};

#endif
