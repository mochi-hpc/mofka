#ifndef BENCHMARK_CONSUMER_H
#define BENCHMARK_CONSUMER_H

#include "Communicator.hpp"

#include <nlohmann/json.hpp>
#include <mpi.h>
#include <thallium.hpp>

class BenchmarkConsumer {

    using json = nlohmann::json;

    thallium::engine m_engine;

    public:

    BenchmarkConsumer(
        thallium::engine engine,
        unsigned seed,
        const json& config,
        Communicator comm,
        bool run_in_thread = false)
    : m_engine{std::move(engine)}
    {

    }

    void run() {

    }

    void wait() {

    }
};

#endif
