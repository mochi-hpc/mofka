/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_THREAD_POOL_IMPL_H
#define MOFKA_THREAD_POOL_IMPL_H

#include "mofka/ThreadPool.hpp"
#include <thallium.hpp>
#include <vector>

namespace mofka {

class ThreadPoolImpl {

    public:

    ThreadPoolImpl(ThreadCount tc)
    : m_pool(
        thallium::pool::create(
            thallium::pool::access::mpmc,
            thallium::pool::kind::fifo_wait))
    {
        for(std::size_t i=0; i < tc.count; ++i) {
            m_xstreams.push_back(
                thallium::xstream::create(
                    thallium::scheduler::predef::basic_wait, *m_pool));

        }
    }

    thallium::managed<thallium::pool>                 m_pool;
    std::vector<thallium::managed<thallium::xstream>> m_xstreams;
};

}

#endif
