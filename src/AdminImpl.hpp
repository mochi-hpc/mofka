/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOFKA_ADMIN_IMPL_H
#define __MOFKA_ADMIN_IMPL_H

#include <thallium.hpp>

namespace mofka {

namespace tl = thallium;

class AdminImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_create_topic;
    tl::remote_procedure m_open_topic;
    tl::remote_procedure m_close_topic;
    tl::remote_procedure m_destroy_topic;

    AdminImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_create_topic(m_engine.define("mofka_create_topic"))
    , m_open_topic(m_engine.define("mofka_open_topic"))
    , m_close_topic(m_engine.define("mofka_close_topic"))
    , m_destroy_topic(m_engine.define("mofka_destroy_topic"))
    {}

    AdminImpl(margo_instance_id mid)
    : AdminImpl(tl::engine(mid)) {
    }

    ~AdminImpl() {}
};

}

#endif
