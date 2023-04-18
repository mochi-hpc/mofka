/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_ADMIN_IMPL_H
#define __ALPHA_ADMIN_IMPL_H

#include <thallium.hpp>

namespace alpha {

namespace tl = thallium;

class AdminImpl {

    public:

    tl::engine           m_engine;
    tl::remote_procedure m_create_resource;
    tl::remote_procedure m_open_resource;
    tl::remote_procedure m_close_resource;
    tl::remote_procedure m_destroy_resource;

    AdminImpl(const tl::engine& engine)
    : m_engine(engine)
    , m_create_resource(m_engine.define("alpha_create_resource"))
    , m_open_resource(m_engine.define("alpha_open_resource"))
    , m_close_resource(m_engine.define("alpha_close_resource"))
    , m_destroy_resource(m_engine.define("alpha_destroy_resource"))
    {}

    AdminImpl(margo_instance_id mid)
    : AdminImpl(tl::engine(mid)) {
    }

    ~AdminImpl() {}
};

}

#endif
