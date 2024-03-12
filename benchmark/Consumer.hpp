#ifndef BENCHMARK_CONSUMER_H
#define BENCHMARK_CONSUMER_H

#include <nlohmann/json.hpp>
#include <mpi.h>

class BenchmarkConsumer {

    using json = nlohmann::json;

    public:

    BenchmarkConsumer(
        unsigned seed,
        const json& config,
        MPI_Comm comm,
        bool run_in_thread = false)
    {

    }

    void run() {

    }

    void wait() {

    }
};

#endif
