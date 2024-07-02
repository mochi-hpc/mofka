#ifndef BENCHMARK_CONSUMER_H
#define BENCHMARK_CONSUMER_H

#include "Communicator.hpp"
#include "Statistics.hpp"
#include "RNG.hpp"

#include <mofka/Client.hpp>
#include <mofka/ServiceHandle.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/Consumer.hpp>

#include <nlohmann/json.hpp>
#include <mpi.h>
#include <thallium.hpp>
#include <spdlog/spdlog.h>
#include <random>

class BenchmarkConsumer {

    using json = nlohmann::json;

    thallium::engine     m_engine;
    mofka::Client        m_mofka_client;
    mofka::ServiceHandle m_mofka_service_handle;
    mofka::TopicHandle   m_mofka_topic_handle;
    mofka::Consumer      m_mofka_consumer;
    Communicator         m_comm;
    json                 m_config;
    RNG                  m_rng;
    bool                 m_run_in_thread;
    bool                 m_has_partitions;
    Statistics           m_pull_stats;
    Statistics           m_ack_stats;
    double               m_runtime;

    double m_data_selectivity    = 1.0;
    double m_data_proportion_min = 1.0;
    double m_data_proportion_max = 1.0;
    size_t m_data_num_blocks_min = 1;
    size_t m_data_num_blocks_max = 1;
    bool   m_check_data = false;

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
    , m_rng(seed*33+42)
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

        if(config.contains("data_selector")) {
            m_data_selectivity = config["data_selector"].value("selectivity", m_data_selectivity);
            if(config["data_selector"].contains("proportion")) {
                auto& proportion = config["data_selector"]["proportion"];
                if(proportion.is_number_float())
                    m_data_proportion_max = m_data_proportion_min = proportion.get<double>();
                else {
                    m_data_proportion_min = proportion[0].get<double>();
                    m_data_proportion_max = proportion[1].get<double>();
                }
            }
        }
        if(config.contains("data_broker")) {
            if(config["data_broker"].contains("num_blocks")) {
                auto& num_blocks = config["data_broker"]["num_blocks"];
                if(num_blocks.is_number_unsigned())
                    m_data_num_blocks_max = m_data_num_blocks_min = num_blocks.get<size_t>();
                else {
                    m_data_num_blocks_min = num_blocks[0].get<size_t>();
                    m_data_num_blocks_max = num_blocks[1].get<size_t>();
                }
            }
        }
        m_check_data = config.value("check_data", m_check_data);

        std::vector<mofka::PartitionInfo> my_partitions;
        size_t comm_size = m_comm.size();
        for(size_t i = m_comm.rank(); i < m_mofka_topic_handle.partitions().size(); i += comm_size) {
            my_partitions.push_back(m_mofka_topic_handle.partitions()[i]);
        }
        m_has_partitions = !my_partitions.empty();
        if(m_has_partitions) {
            spdlog::trace("[consumer] Creating consumer \"{}\" associated with {} partitions",
                          consumer_name, my_partitions.size());
            mofka::DataSelector my_selector =
                [this](const mofka::Metadata& md, const mofka::DataDescriptor& dd) {
                    return selector(md, dd);
                };
            mofka::DataBroker my_broker =
                [this](const mofka::Metadata& md, const mofka::DataDescriptor& dd) {
                    return broker(md, dd);
                };
            m_mofka_consumer = m_mofka_topic_handle.consumer(
                consumer_name, batch_size, thread_pool, my_partitions,
                my_selector, my_broker);
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

    mofka::DataDescriptor selector(const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
        std::bernoulli_distribution dist{m_data_selectivity};
        if(!dist(m_rng)) {
            return mofka::DataDescriptor::Null();
        }
        if(m_data_proportion_min >= 1.0) {
            return descriptor;
        }
        std::uniform_int_distribution<size_t> sizeDistribution(
            m_data_proportion_min*descriptor.size(),
            m_data_proportion_max*descriptor.size());
        size_t selected_size = sizeDistribution(m_rng);
        return descriptor.makeSubView(0, selected_size);
    }

    mofka::Data broker(const mofka::Metadata&, const mofka::DataDescriptor& descriptor) {
        std::uniform_int_distribution<size_t> numBlocksDistribution(
            m_data_num_blocks_min, m_data_num_blocks_max);
        auto num_blocks = std::min(descriptor.size(), numBlocksDistribution(m_rng));
        auto remaining_size = descriptor.size();
        auto block_size = remaining_size/num_blocks;
        std::vector<std::vector<char>>* blocks = new std::vector<std::vector<char>>();
        blocks->reserve(num_blocks);
        while(remaining_size != 0) {
            auto this_block_size = std::min(remaining_size, block_size);
            remaining_size -= this_block_size;
            blocks->emplace_back(this_block_size, '\0');
        }
        std::vector<mofka::Data::Segment> segments{blocks->size()};
        for(size_t i = 0; i < blocks->size(); ++i) {
            segments[i] = mofka::Data::Segment{blocks->at(i).data(), blocks->at(i).size()};
        }
        return mofka::Data{segments, blocks,
            [](void* ctx) {
                delete static_cast<decltype(blocks)*>(ctx);}};
    }

    private:

    void runThread() {
        std::unordered_map<mofka::UUID, size_t> events_received;
        auto num_events = m_config.value("num_events", (ssize_t)(-1));
        auto ack        = m_config.value("ack_every", num_events);
        m_comm.barrier();
        spdlog::info("[consumer] Starting to consume events");
        double t_start = MPI_Wtime();
        double t1, t2;
        if(m_has_partitions) {
            for(ssize_t i = 0; i < num_events || num_events == -1; ++i) {
                spdlog::trace("[consumer] Pulling event {}", i);
                t1 = MPI_Wtime();
                auto event = m_mofka_consumer.pull().wait();
                t2 = MPI_Wtime();
                m_pull_stats << (t2 - t1);
                spdlog::trace("[consumer] Done pulling event {}: received event {} from partition {}",
                             i, event.id(), event.partition().uuid().to_string());
                if(event.id() == mofka::NoMoreEvents) break;
                auto uuid = event.partition().uuid();
                auto it = events_received.find(uuid);
                if(it == events_received.end())
                    it = events_received.insert(std::make_pair(uuid, 0)).first;
                it->second += 1;
                if(it->second % ack == 0 || (num_events != -1 && i == num_events-1)) {
                    spdlog::trace("[consumer] Acknowledging event {}", i);
                    t1 = MPI_Wtime();
                    event.acknowledge();
                    t2 = MPI_Wtime();
                    m_ack_stats << (t2 - t1);
                    spdlog::trace("[consumer] Done acknowledging event {}", i);
                }
                if(m_check_data) checkData(event);
            }
        }
        double t_end = MPI_Wtime();
        spdlog::info("[consumer] Local consumer finished in {} seconds", (t_end - t_start));
        m_comm.barrier();
        t_end = MPI_Wtime();
        m_runtime = t_end - t_start;
        spdlog::info("[consumer] Consumer app finished in {} seconds", (t_end - t_start));
    }

    void checkData(const mofka::Event& event) {
        auto data = event.data();
        size_t x = 0;
        unsigned s = 0;
        for(auto& seg : data.segments()) {
            auto seg_data = static_cast<const char*>(seg.ptr);
            for(unsigned i=0; i < seg.size; ++i) {
                if(seg_data[i] != ('A' + (x % 26))) {
                    throw mofka::Exception{"Invalid data found in consumer"};
                    return;
                }
                x += 1;
            }
            s += 1;
        }
    }

    public:

    json getStatistics() const {
        auto result = json::object();
        result["runtime"] = m_runtime;
        result["pull"] = m_pull_stats.to_json();
        result["ack"] = m_ack_stats.to_json();
        return result;
    }
};

#endif
