/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_DRIVER_IMPL_H
#define MOFKA_KAFKA_DRIVER_IMPL_H

#include "mofka/Exception.hpp"
#include <librdkafka/rdkafka.h>
#include <unordered_map>
#include <thallium.hpp>
#include <vector>

namespace mofka {

class KafkaDriverImpl {

    public:

    bool             m_initialized_abt = false;
    rd_kafka_conf_t* m_kafka_config = nullptr;

    KafkaDriverImpl(const std::string& bootstrap_servers) {
        if(!ABT_initialized()) {
            setenv("ABT_MEM_MAX_NUM_STACKS", "8", 0);
            setenv("ABT_THREAD_STACKSIZE", "2097152", 0);
            ABT_init(0, NULL);
            m_initialized_abt = true;
        }
        char errstr[512];
        m_kafka_config = rd_kafka_conf_new();
        auto ret = rd_kafka_conf_set(m_kafka_config, "bootstrap.servers", bootstrap_servers.c_str(),
                                     errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK) {
            throw Exception{"Could not set Kafka configuration: " + std::string(errstr)};
        }
    }

    ~KafkaDriverImpl() {
        if(m_initialized_abt)
            ABT_finalize();
        rd_kafka_conf_destroy(m_kafka_config);
    }
};

}

#endif
