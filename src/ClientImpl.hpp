/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOFKA_CLIENT_IMPL_H
#define __MOFKA_CLIENT_IMPL_H

#include <thallium.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace mofka {

namespace tl = thallium;

class ClientImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_check_topic;
    tl::remote_procedure m_say_hello;
    tl::remote_procedure m_compute_sum;

    ClientImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_check_topic(m_engine.define("mofka_check_topic"))
    , m_say_hello(m_engine.define("mofka_say_hello").disable_response())
    , m_compute_sum(m_engine.define("mofka_compute_sum"))
    {}

    ClientImpl(margo_instance_id mid)
    : ClientImpl(tl::engine(mid)) {}

    ~ClientImpl() {}
};

}

#endif
