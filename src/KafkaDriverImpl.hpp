/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_KAFKA_DRIVER_IMPL_H
#define MOFKA_KAFKA_DRIVER_IMPL_H

#include "mofka/Exception.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <unordered_map>
#include <thallium.hpp>
#include <vector>

namespace mofka {

class KafkaDriverImpl {

    public:

    bool           m_initialized_abt = false;
    RdKafka::Conf* m_kafka_config = nullptr;
    std::string    m_rest_url;

    KafkaDriverImpl(const std::string& bootstrap_servers,
                    std::string rest_url)
    : m_rest_url{std::move(rest_url)} {
        if(!ABT_initialized()) {
            setenv("ABT_MEM_MAX_NUM_STACKS", "8", 0);
            setenv("ABT_THREAD_STACKSIZE", "2097152", 0);
            ABT_init(0, NULL);
            m_initialized_abt = true;
        }
        m_kafka_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        std::string errstr;
        auto ret = m_kafka_config->set("bootstrap.servers", bootstrap_servers.c_str(), errstr);
        if(ret != RdKafka::Conf::CONF_OK)
             throw Exception{"Could not set Mofka configuration: " + errstr};
    }

    ~KafkaDriverImpl() {
        if(m_initialized_abt)
            ABT_finalize();
        delete m_kafka_config;
    }
};

}

#endif
