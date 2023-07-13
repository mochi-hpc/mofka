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

    ThreadPoolImpl(ThreadCount tc) {
        if(tc.count == 0) {
            m_pool = thallium::xstream::self().get_main_pools(1)[0];
        } else {
            m_managed_pool = thallium::pool::create(
                thallium::pool::access::mpmc,
                thallium::pool::kind::fifo_wait);
            m_pool = *m_managed_pool;
            for(std::size_t i=0; i < tc.count; ++i) {
                auto sched = thallium::scheduler::predef::basic_wait;
                m_managed_xstreams.push_back(thallium::xstream::create(sched, m_pool));
            }
        }
    }

    ~ThreadPoolImpl() {
        for(auto& x : m_managed_xstreams) {
            x->join();
        }
    }

    template<typename Function>
    void pushWork(Function&& func) {
        m_pool.make_thread(std::forward<Function>(func), thallium::anonymous{});
        if(m_managed_xstreams.empty()) thallium::thread::yield();
    }

    private:

    thallium::pool                                    m_pool;
    /* Argobots object this ThreadPoolImpl has responsibility for freeing */
    thallium::managed<thallium::pool>                 m_managed_pool;
    std::vector<thallium::managed<thallium::xstream>> m_managed_xstreams;
};

}

#endif
