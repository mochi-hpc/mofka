#ifndef BENCHMARK_PRODUCER_H
#define BENCHMARK_PRODUCER_H

#include "MetadataGenerator.hpp"
#include <nlohmann/json.hpp>
#include <mpi.h>

class BenchmarkProducer {

    using json = nlohmann::json;

    StringGenerator   m_string_generator;
    MetadataGenerator m_metadata_generator;

    public:

    BenchmarkProducer(
        unsigned seed,
        const json& config,
        MPI_Comm comm,
        bool run_in_thread = false)
    : m_string_generator(seed)
    , m_metadata_generator(
        m_string_generator,
        config["topic"]["metadata"]["num_fields"],
        getMin(config["topic"]["metadata"]["key_sizes"]),
        getMax(config["topic"]["metadata"]["key_sizes"]),
        getMin(config["topic"]["metadata"]["val_sizes"]),
        getMax(config["topic"]["metadata"]["val_sizes"]))
    {
        
    }

    void run() {

    }

    void wait() {

    }

    private:

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
            return std::min<size_t>(
                v[0].get<size_t>(),
                v[1].get<size_t>());
        }
    }
};

#endif
